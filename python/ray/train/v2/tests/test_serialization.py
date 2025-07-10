import sys

import pytest

from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


def test_captured_imports(ray_start_4_cpus):
    """Test that imports in closures are properly handled during serialization."""
    import torch

    # Create a closure that captures torch
    def capture_torch_import_fn():
        # torch is captured in the closure of the train_fn
        # and should be re-imported on each worker.
        result = torch.ones(1)
        # Verify torch is available on workers
        assert result.shape == (1,)

    trainer = DataParallelTrainer(
        capture_torch_import_fn,
        run_config=RunConfig(),
        scaling_config=ScalingConfig(num_workers=2),
    )
    # This should succeed - torch should be available on workers
    result = trainer.fit()
    assert not result.error


def test_deserialization_error(ray_start_4_cpus):
    """Test that train_fn deserialization errors are propagated properly.

    This test showcases a deserialization error example where a required
    module is not available on worker nodes.
    """

    def failing_import_fn():
        # Try to import a module that doesn't exist
        # This simulates the case where a dependency is available on the driver
        # but not on worker nodes
        import nonexistent_module_for_testing

        nonexistent_module_for_testing.some_function()

    trainer = DataParallelTrainer(
        failing_import_fn,
        run_config=RunConfig(),
        scaling_config=ScalingConfig(num_workers=2),
    )

    # This should fail with a task error due to import failure
    with pytest.raises(Exception) as exc_info:
        trainer.fit()

    # The error should contain information about the missing module
    error_str = str(exc_info.value)
    assert "nonexistent_module_for_testing" in error_str


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
