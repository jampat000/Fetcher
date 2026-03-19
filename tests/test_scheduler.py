from app.scheduler import ServiceScheduler


def test_shutdown_ignores_runtime_error_when_loop_closed() -> None:
    scheduler = ServiceScheduler()

    class _FakeScheduler:
        running = True

        @staticmethod
        def shutdown(wait: bool = False) -> None:  # noqa: ARG001
            raise RuntimeError("Event loop is closed")

    scheduler._sched = _FakeScheduler()

    # Should not raise during teardown scenarios.
    scheduler.shutdown()
