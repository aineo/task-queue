from TaskQueue import Resources, Task, TaskQueue
import unittest


class TestTaskQueue(unittest.TestCase):
    def setUp(self) -> None:
        # Init class
        self.task_queue = TaskQueue()

    def test_is_instance(self):
        # Test is instance
        self.assertIsInstance(self.task_queue, TaskQueue)

    def test_add_task(self):
        # Test for adding two tasks by the publisher, check number of tasks in queue
        self.task_queue.add_task(
            1,
            Resources(8, 4, 4),
            "Build project in Docker containers",
            "Succesfully builded project",
        )
        self.task_queue.add_task(
            3, Resources(4, 2, 1), "Run database", "Succesfully runned database"
        )
        self.assertEqual(len(self.task_queue.queue), 2)

    def test_get_task(self):
        # Test for getting task for consumer with highest priority and sufficient resources
        self.task_queue.add_task(
            1,
            Resources(8, 4, 4),
            "Build project in Docker containers",
            "Succesfully builded project",
        )
        self.task_queue.add_task(
            3, Resources(4, 2, 1), "Run database", "Succesfully runned database"
        )
        self.assertEqual(
            self.task_queue.get_task(Resources(10, 6, 6)),
            Task(
                0,
                1,
                Resources(8, 4, 4),
                "Build project in Docker containers",
                "Succesfully builded project",
            ),
        )

    def test_get_task_empty_queue(self):
        # Test of attempt to get task from empty queue
        self.assertEqual(self.task_queue.get_task(Resources(10, 6, 6)), None)

    def test_get_task_low_resources(self):
        # Test for getting task for consumer with highest priority and unsufficient resources
        self.task_queue.add_task(
            1,
            Resources(8, 4, 4),
            "Build project in Docker containers",
            "Succesfully builded project",
        )
        self.task_queue.add_task(
            3, Resources(4, 2, 1), "Run database", "Succesfully runned database"
        )
        self.assertEqual(self.task_queue.get_task(Resources(2, 1, 1)), None)

    def test_add_get_task_sequence(self):
        # Test sequential add/get of tasks from task queue
        self.task_queue.add_task(
            1,
            Resources(8, 4, 4),
            "Build project in Docker containers",
            "Succesfully builded project",
        )
        self.task_queue.add_task(
            3, Resources(4, 2, 1), "Run database", "Succesfully runned database"
        )
        self.task_queue.get_task(Resources(10, 6, 6))

        self.task_queue.add_task(
            4,
            Resources(32, 8, 16),
            "Train & test NLP model",
            "Neural network trained with sufficient accuracy",
        )
        self.task_queue.get_task(Resources(64, 10, 32))

        self.assertEqual(
            self.task_queue.get_task(Resources(64, 10, 32)),
            Task(
                2,
                4,
                Resources(32, 8, 16),
                "Train & test NLP model",
                "Neural network trained with sufficient accuracy",
            ),
        )
        # While all tasks are distributed
        self.assertEqual(len(self.task_queue.queue), 0)


if __name__ == "__main__":
    unittest.main()
