"""
Chaos Scheduler Service

Manages chaos trigger evaluation and execution.
Supports event-based, time-based, and count-based triggers.
"""

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from uuid import UUID

import yaml
from structlog import get_logger

from chaostrace.control_plane.models.chaos import (
    ChaosAction,
    ChaosScenario,
    ChaosState,
    ChaosTrigger,
    TriggerType,
)
from chaostrace.control_plane.models.events import SQLEvent
from chaostrace.db_proxy.chaos_hooks import ChaosHooks

logger = get_logger(__name__)


class ChaosScheduler:
    """
    Schedules and executes chaos events based on triggers.
    
    The scheduler monitors:
    - SQL events (for event-based triggers)
    - Elapsed time (for time-based triggers)
    - Event counts (for count-based triggers)
    
    When conditions are met, it invokes the chaos hooks to
    inject failures into the sandbox environment.
    
    Usage:
        scheduler = ChaosScheduler.from_file("chaos_scripts/db_lock_v1.yaml")
        scheduler.start(run_id)
        
        # On each SQL event:
        await scheduler.on_event(sql_event)
        
        # Periodically:
        await scheduler.check_time_triggers()
        
        scheduler.stop()
    """
    
    def __init__(
        self,
        scenario: ChaosScenario,
        chaos_hooks: ChaosHooks | None = None,
        event_callback: Callable | None = None,
    ):
        """
        Initialize the chaos scheduler.
        
        Args:
            scenario: The chaos scenario to execute.
            chaos_hooks: Hooks for executing chaos actions.
            event_callback: Callback for chaos events.
        """
        self.scenario = scenario
        self.chaos_hooks = chaos_hooks or ChaosHooks()
        self.event_callback = event_callback
        
        self._state: ChaosState | None = None
        self._running = False
        self._time_check_task: asyncio.Task | None = None
        
        logger.info(
            "chaos_scheduler_initialized",
            scenario=scenario.name,
            trigger_count=len(scenario.triggers),
        )
    
    @classmethod
    def from_file(cls, path: Path | str, **kwargs) -> "ChaosScheduler":
        """Load a chaos scenario from a YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Chaos script not found: {path}")
        
        with open(path) as f:
            data = yaml.safe_load(f)
        
        scenario = ChaosScenario(**data)
        return cls(scenario, **kwargs)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any], **kwargs) -> "ChaosScheduler":
        """Create a scheduler from a dictionary."""
        scenario = ChaosScenario(**data)
        return cls(scenario, **kwargs)
    
    def start(self, run_id: UUID) -> None:
        """
        Start the chaos scheduler for a run.
        
        Args:
            run_id: The ID of the run to attach to.
        """
        if self._running:
            return
        
        self._state = ChaosState(
            scenario_name=self.scenario.name,
            run_id=str(run_id),
            started_at=datetime.utcnow(),
        )
        self._running = True
        
        # Start time-based trigger monitoring
        self._time_check_task = asyncio.create_task(self._time_check_loop())
        
        logger.info(
            "chaos_scheduler_started",
            run_id=str(run_id),
            scenario=self.scenario.name,
        )
    
    def stop(self) -> None:
        """Stop the chaos scheduler."""
        self._running = False
        
        if self._time_check_task:
            self._time_check_task.cancel()
            self._time_check_task = None
        
        logger.info("chaos_scheduler_stopped")
    
    async def on_event(self, event: SQLEvent | dict) -> list[ChaosAction]:
        """
        Process an event and check for triggered chaos.
        
        Args:
            event: The SQL or other event to process.
            
        Returns:
            List of chaos actions that were triggered.
        """
        if not self._running or not self._state:
            return []
        
        triggered_actions = []
        
        # Extract event info
        if isinstance(event, dict):
            event_type = event.get("event_type", "")
            sql_type = event.get("sql_type", "")
            tables = event.get("tables", [])
        else:
            event_type = event.event_type.value if hasattr(event, 'event_type') else ""
            sql_type = event.sql_type.value if hasattr(event, 'sql_type') else ""
            tables = event.tables if hasattr(event, 'tables') else []
        
        # Update event counts
        event_key = f"{event_type}:{sql_type}"
        self._state.event_counts[event_key] = (
            self._state.event_counts.get(event_key, 0) + 1
        )
        
        # Check each trigger
        for trigger in self.scenario.triggers:
            if not trigger.enabled:
                continue
            
            if trigger.trigger_type == TriggerType.EVENT:
                action = await self._check_event_trigger(trigger, event_type, sql_type, tables)
                if action:
                    triggered_actions.append(action)
            
            elif trigger.trigger_type == TriggerType.COUNT:
                action = await self._check_count_trigger(trigger, event_type, sql_type)
                if action:
                    triggered_actions.append(action)
        
        return triggered_actions
    
    async def _check_event_trigger(
        self,
        trigger: ChaosTrigger,
        event_type: str,
        sql_type: str,
        tables: list[str],
    ) -> ChaosAction | None:
        """Check if an event trigger should fire."""
        if not trigger.event_condition:
            return None
        
        condition = trigger.event_condition
        
        # Check event type match
        if condition.event_type.upper() not in event_type.upper():
            return None
        
        # Check SQL type match
        if condition.parsed_type:
            if condition.parsed_type.upper() != sql_type.upper():
                return None
        
        # Check table pattern
        if condition.table_pattern:
            matched = any(
                condition.table_pattern.lower() in t.lower()
                for t in tables
            )
            if not matched:
                return None
        
        # Check occurrence
        trigger_key = trigger.name or f"trigger_{id(trigger)}"
        fire_count = self._state.trigger_fire_counts.get(trigger_key, 0)
        
        if condition.occurrence == "first" and fire_count > 0:
            return None
        elif isinstance(condition.occurrence, int):
            if fire_count < condition.occurrence - 1:
                return None
        
        # Check max triggers
        if fire_count >= trigger.max_triggers:
            return None
        
        # Check cooldown
        if trigger.cooldown_seconds > 0:
            last_fired = self._state.trigger_last_fired.get(trigger_key)
            if last_fired:
                elapsed = (datetime.utcnow() - last_fired).total_seconds()
                if elapsed < trigger.cooldown_seconds:
                    return None
        
        # Trigger!
        return await self._execute_trigger(trigger, trigger_key, {"tables": tables})
    
    async def _check_count_trigger(
        self,
        trigger: ChaosTrigger,
        event_type: str,
        sql_type: str,
    ) -> ChaosAction | None:
        """Check if a count trigger should fire."""
        if not trigger.count_condition:
            return None
        
        condition = trigger.count_condition
        event_key = f"{condition.event_type}:{sql_type}"
        
        count = self._state.event_counts.get(event_key, 0)
        
        if count < condition.count:
            return None
        
        trigger_key = trigger.name or f"trigger_{id(trigger)}"
        fire_count = self._state.trigger_fire_counts.get(trigger_key, 0)
        
        if fire_count >= trigger.max_triggers:
            return None
        
        # Reset count if configured
        if condition.reset_after_trigger:
            self._state.event_counts[event_key] = 0
        
        return await self._execute_trigger(trigger, trigger_key, {})
    
    async def _time_check_loop(self) -> None:
        """Periodically check time-based triggers."""
        while self._running:
            try:
                await asyncio.sleep(1)  # Check every second
                
                if not self._state:
                    continue
                
                elapsed = (datetime.utcnow() - self._state.started_at).total_seconds()
                
                for trigger in self.scenario.triggers:
                    if not trigger.enabled:
                        continue
                    
                    if trigger.trigger_type != TriggerType.TIME:
                        continue
                    
                    if not trigger.time_condition:
                        continue
                    
                    trigger_key = trigger.name or f"trigger_{id(trigger)}"
                    fire_count = self._state.trigger_fire_counts.get(trigger_key, 0)
                    
                    if fire_count >= trigger.max_triggers:
                        continue
                    
                    condition = trigger.time_condition
                    trigger_time = condition.elapsed_seconds
                    
                    # Add jitter if configured
                    if condition.jitter_seconds > 0:
                        import random
                        trigger_time += random.randint(0, condition.jitter_seconds)
                    
                    # Check if we've reached the trigger time
                    if elapsed >= trigger_time:
                        # Only fire once
                        if fire_count == 0:
                            await self._execute_trigger(trigger, trigger_key, {})
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("time_check_error", error=str(e))
    
    async def _execute_trigger(
        self,
        trigger: ChaosTrigger,
        trigger_key: str,
        context: dict,
    ) -> ChaosAction:
        """Execute a trigger's chaos action."""
        # Update state
        self._state.trigger_fire_counts[trigger_key] = (
            self._state.trigger_fire_counts.get(trigger_key, 0) + 1
        )
        self._state.trigger_last_fired[trigger_key] = datetime.utcnow()
        self._state.total_chaos_events += 1
        
        action = trigger.action
        
        logger.info(
            "chaos_triggered",
            trigger=trigger_key,
            action_type=action.type.value,
            fire_count=self._state.trigger_fire_counts[trigger_key],
        )
        
        # Execute the chaos action
        try:
            result = await self.chaos_hooks.execute(action, context)
            
            if self.event_callback:
                await self.event_callback({
                    "event_type": "chaos_triggered",
                    "trigger": trigger_key,
                    "action_type": action.type.value,
                    "result": result,
                })
            
        except Exception as e:
            logger.exception(
                "chaos_execution_failed",
                trigger=trigger_key,
                error=str(e),
            )
        
        return action
    
    @property
    def state(self) -> ChaosState | None:
        """Get current chaos state."""
        return self._state
    
    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
    
    def get_stats(self) -> dict:
        """Get scheduler statistics."""
        if not self._state:
            return {}
        
        return {
            "scenario": self.scenario.name,
            "total_chaos_events": self._state.total_chaos_events,
            "trigger_fire_counts": dict(self._state.trigger_fire_counts),
            "event_counts": dict(self._state.event_counts),
        }
