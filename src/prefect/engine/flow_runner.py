from typing import Any, Callable, Dict, Iterable, Iterator, NamedTuple, Optional, Set
from contextlib import contextmanager

import pendulum
import prefect
from prefect.core import Edge, Flow, Task
from prefect.engine.result import Result
from prefect.engine.results import ConstantResult
from prefect.engine.runner import ENDRUN, Runner, call_state_handlers
from prefect.engine.state import Failed, Mapped, Pending, Running, Scheduled, State, Success
from prefect.utilities import executors
from prefect.utilities.collections import flatten_seq

FlowRunnerInitializeResult = NamedTuple(
    "FlowRunnerInitializeResult",
    [
        ("state", State),
        ("task_states", Dict[Task, State]),
        ("context", Dict[str, Any]),
        ("task_contexts", Dict[Task, Dict[str, Any]]),
    ],
)

class FlowRunner(Runner):
    def __init__(
        self,
        flow: Flow,
        task_runner_cls: type = None,
        state_handlers: Iterable[Callable] = None,
    ):
        self.flow = flow
        self.task_runner_cls = task_runner_cls or prefect.engine.get_default_task_runner_class()
        super().__init__(state_handlers=state_handlers)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.flow.name}>"

    def call_runner_target_handlers(self, old_state: State, new_state: State) -> State:
        self.logger.debug(
            f"Flow '{self.flow.name}': Handling state change from {type(old_state).__name__} to {type(new_state).__name__}"
        )
        for handler in self.flow.state_handlers:
            new_state = handler(self.flow, old_state, new_state) or new_state
        return new_state

    def initialize_run(
        self,
        state: Optional[State],
        task_states: Dict[Task, State],
        context: Dict[str, Any],
        task_contexts: Dict[Task, Dict[str, Any]],
        parameters: Dict[str, Any],
    ) -> FlowRunnerInitializeResult:
        context_params = context.setdefault("parameters", {})
        for p in self.flow.parameters():
            if not p.required:
                context_params.setdefault(p.name, p.default)
        context_params.update(parameters or {})

        context.update(flow_name=self.flow.name, scheduled_start_time=pendulum.now("utc"))

        now = pendulum.now("utc")
        dates = {
            "date": now,
            "today": now.strftime("%Y-%m-%d"),
            "yesterday": now.add(days=-1).strftime("%Y-%m-%d"),
            "tomorrow": now.add(days=1).strftime("%Y-%m-%d"),
            "today_nodash": now.strftime("%Y%m%d"),
            "yesterday_nodash": now.add(days=-1).strftime("%Y%m%d"),
            "tomorrow_nodash": now.add(days=1).strftime("%Y%m%d"),
        }
        context.update(dates)

        for task in self.flow.tasks:
            task_contexts.setdefault(task, {}).update(task_name=task.name, task_slug=self.flow.slugs[task])

        state, context = super().initialize_run(state=state, context=context)
        return FlowRunnerInitializeResult(state, task_states, context, task_contexts)

    def run(
        self,
        state: State = None,
        task_states: Dict[Task, State] = None,
        return_tasks: Iterable[Task] = None,
        parameters: Dict[str, Any] = None,
        task_runner_state_handlers: Iterable[Callable] = None,
        executor: "prefect.engine.executors.Executor" = None,
        context: Dict[str, Any] = None,
        task_contexts: Dict[Task, Dict[str, Any]] = None,
    ) -> State:
        self.logger.info(f"Beginning Flow run for '{self.flow.name}'")

        task_states = dict(task_states or {})
        context = dict(context or {})
        task_contexts = dict(task_contexts or {})
        parameters = dict(parameters or {})
        executor = executor or getattr(self.flow, "executor", None) or prefect.engine.get_default_executor_class()()

        self.logger.debug("Using executor type %s", type(executor).__name__)

        try:
            state, task_states, context, task_contexts = self.initialize_run(
                state, task_states, context, task_contexts, parameters
            )

            with prefect.context(context):
                state = self.check_flow_is_pending_or_running(state)
                state = self.check_flow_reached_start_time(state)
                state = self.set_flow_to_running(state)
                state = self.get_flow_run_state(
                    state, task_states, task_contexts, return_tasks, task_runner_state_handlers, executor
                )

        except ENDRUN as exc:
            state = exc.state
        except Exception as exc:
            self.logger.exception(f"Unexpected error while running flow: {repr(exc)}")
            if prefect.context.get("raise_on_exception"):
                raise exc
            state = self.handle_state_change(state or Pending(), Failed(message=f"Unexpected error: {repr(exc)}", result=exc))

        return state

    @contextmanager
    def check_for_cancellation(self) -> Iterator:
        yield

    @call_state_handlers
    def check_flow_reached_start_time(self, state: State) -> State:
        if isinstance(state, Scheduled) and state.start_time and state.start_time > pendulum.now("utc"):
            self.logger.debug(f"Flow '{self.flow.name}': start_time has not been reached; ending run.")
            raise ENDRUN(state)
        return state

    @call_state_handlers
    def check_flow_is_pending_or_running(self, state: State) -> State:
        if state.is_finished():
            self.logger.info("Flow run has already finished.")
            raise ENDRUN(state)
        elif not (state.is_pending() or state.is_running()):
            self.logger.info("Flow is not ready to run.")
            raise ENDRUN(state)
        return state

    @call_state_handlers
    def set_flow_to_running(self, state: State) -> State:
        if state.is_pending():
            return Running(message="Running flow.")
        elif state.is_running():
            return state
        raise ENDRUN(state)

    @executors.run_with_heartbeat
    @call_state_handlers
    def get_flow_run_state(
        self,
        state: State,
        task_states: Dict[Task, State],
        task_contexts: Dict[Task, Dict[str, Any]],
        return_tasks: Set[Task],
        task_runner_state_handlers: Iterable[Callable],
        executor: "prefect.engine.executors.base.Executor",
    ) -> State:
        mapped_children = {}

        if not state.is_running():
            self.logger.info("Flow is not in a Running state.")
            raise ENDRUN(state)

        if return_tasks is None:
            return_tasks = set()
        if set(return_tasks).difference(self.flow.tasks):
            raise ValueError("Some tasks in return_tasks were not found in the flow.")

        def extra_context(task: Task, task_index: int = None) -> dict:
            return {"task_name": task.name, "task_tags": task.tags, "task_index": task_index}

        with self.check_for_cancellation(), executor.start():
            for task in self.flow.sorted_tasks():
                task_state = task_states.get(task)

                if task_state is None and isinstance(task, prefect.tasks.core.constants.Constant):
                    task_states[task] = task_state = Success(result=task.value)

                if isinstance(task_state, State) and task_state.is_finished() and not task_state.is_cached() and not task_state.is_mapped():
                    continue

                upstream_states = {}
                upstream_mapped_states = {}

                for edge in self.flow.edges_to(task):
                    upstream_states[edge] = task_states.get(edge.upstream_task, Pending(message="Task state not available."))

                    if edge.flattened and not isinstance(upstream_states[edge], Mapped):
                        upstream_states[edge] = executor.submit(executors.flatten_upstream_state, upstream_states[edge])

                    if not edge.mapped and isinstance(upstream_states[edge], Mapped):
                        children = mapped_children.get(edge.upstream_task, [])
                        if edge.flattened:
                            children = executors.flatten_mapped_children(mapped_children=children, executor=executor)
                        upstream_mapped_states[edge] = children

                for key, val in self.flow.constants[task].items():
                    edge = Edge(upstream_task=prefect.tasks.core.constants.Constant(val), downstream_task=task, key=key)
                    upstream_states[edge] = Success("Auto-generated constant value", result=ConstantResult(value=val))

                if any(edge.mapped for edge in upstream_states.keys()):
                    upstream_states = executor.wait({e: state for e, state in upstream_states.items()})
                    task_states[task] = executor.wait(executor.submit(
                        run_task,
                        task=task,
                        state=task_state,
                        upstream_states=upstream_states,
                        context=dict(prefect.context, **task_contexts.get(task, {})),
                        flow_result=self.flow.result,
                        task_runner_cls=self.task_runner_cls,
                        task_runner_state_handlers=task_runner_state_handlers,
                        upstream_mapped_states=upstream_mapped_states,
                        is_mapped_parent=True,
                        extra_context=extra_context(task),
                    ))

                    list_of_upstream_states = executors.prepare_upstream_states_for_mapping(
                        task_states[task], upstream_states, mapped_children, executor=executor
                    )

                    submitted_states = []
                    for idx, states in enumerate(list_of_upstream_states):
                        current_state = task_state.map_states[idx] if isinstance(task_state, Mapped) and len(task_state.map_states) >= idx + 1 else task_state
                        submitted_states.append(executor.submit(
                            run_task,
                            task=task,
                            state=current_state,
                            upstream_states=states,
                            context=dict(prefect.context, **task_contexts.get(task, {}), map_index=idx),
                            flow_result=self.flow.result,
                            task_runner_cls=self.task_runner_cls,
                            task_runner_state_handlers=task_runner_state_handlers,
                            upstream_mapped_states=upstream_mapped_states,
                            extra_context=extra_context(task, task_index=idx),
                        ))
                    if isinstance(task_states.get(task), Mapped):
                        mapped_children[task] = submitted_states

                else:
                    task_states[task] = executor.submit(
                        run_task,
                        task=task,
                        state=task_state,
                        upstream_states=upstream_states,
                        context=dict(prefect.context, **task_contexts.get(task, {})),
                        flow_result=self.flow.result,
                        task_runner_cls=self.task_runner_cls,
                        task_runner_state_handlers=task_runner_state_handlers,
                        upstream_mapped_states=upstream_mapped_states,
                        extra_context=extra_context(task),
                    )

            final_tasks = self.flow.terminal_tasks().union(self.flow.reference_tasks()).union(return_tasks)
            final_states = executor.wait({
                t: task_states.get(t, Pending("Task not evaluated by FlowRunner.")) for t in final_tasks
            })

            all_final_states = final_states.copy()
            for t, s in list(final_states.items()):
                if s.is_mapped():
                    if t in mapped_children:
                        s.map_states = executor.wait(mapped_children[t])
                    s.result = [ms.result for ms in s.map_states]
                    all_final_states[t] = s.map_states

            key_states = set(flatten_seq([all_final_states[t] for t in self.flow.reference_tasks()]))
            terminal_states = set(flatten_seq([all_final_states[t] for t in self.flow.terminal_tasks()]))
            return_states = {t: final_states[t] for t in return_tasks}

            state = self.determine_final_state(state, key_states, return_states, terminal_states)
            return state

    def determine_final_state(
        self,
        state: State,
        key_states: Set[State],
        return_states: Dict[Task, State],
        terminal_states: Set[State],
    ) -> State:
        if not all(s.is_finished() for s in terminal_states):
            self.logger.info("Flow run RUNNING: terminal tasks are incomplete.")
            state.result = return_states
        elif any(s.is_failed() for s in key_states):
            self.logger.info("Flow run FAILED: some reference tasks failed.")
            state = Failed(message="Some reference tasks failed.", result=return_states)
        elif all(s.is_successful() for s in key_states):
            self.logger.info("Flow run SUCCESS: all reference tasks succeeded")
            state = Success(message="All reference tasks succeeded.", result=return_states)
        else:
            self.logger.info("Flow run SUCCESS: no reference tasks failed")
            state = Success(message="No reference tasks failed.", result=return_states)
        return state

def run_task(
    task: Task,
    state: State,
    upstream_states: Dict[Edge, State],
    context: Dict[str, Any],
    flow_result: Result,
    task_runner_cls: Callable,
    task_runner_state_handlers: Iterable[Callable],
    upstream_mapped_states: Dict[Edge, list],
    is_mapped_parent: bool = False,
) -> State:
    with prefect.context(context):
        for edge, upstream_state in upstream_states.items():
            if not edge.mapped and upstream_state.is_mapped():
                assert isinstance(upstream_state, Mapped)
                upstream_state.map_states = upstream_mapped_states.get(edge, upstream_state.map_states)
                upstream_state.result = [s.result for s in upstream_state.map_states]
        task_runner = task_runner_cls(
            task=task,
            state_handlers=task_runner_state_handlers,
            flow_result=flow_result,
        )
        return task_runner.run(
            state=state,
            upstream_states=upstream_states,
            is_mapped_parent=is_mapped_parent,
            context=context,
        )