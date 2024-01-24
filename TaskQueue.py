from dataclasses import dataclass
from heapq import heappop, heappush

@dataclass
class Resources:
    """Class of resources required to complete a task

    Attributes
    ----------
    ram (int): RAM
    cpu_cores (int): Number of cores CPU
    gpu_cores (int): Number of cores GPU
    """

    ram: int
    cpu_cores: int
    gpu_cores: int


@dataclass
class Task:
    """Class describing a task in a queue

    Attributes
    ----------
    id (int): Task ID
    priority (int): Priority (0 - highest)
    resources (Resources): Resources required to complete a task
    content (str): Task description
    result (str): Result description
    """

    id: int
    priority: int
    resources: Resources
    content: str
    result: str


class TaskQueue:
    """Queue task class
    Uses priority queue algorithm
    Implemented in the standard library in the heapq module

    Attributes
    ----------
    queue (list): task queue
    __id (int, private): task id

    Methods
    -------
    __init__(self)
      Constructs all the necessary attributes for the task queue object

    add_task(self, priority, resources, content, result)
      Adds a task to queue by the publisher

    get_task(self, available_resources)
      Retrieves a task from queue for the consumer
    """

    def __init__(self):
        self.queue = []
        self.__id = 0

    # def __del__(self):
    #  print('Class was destroyed')

    def add_task(self, priority: int, resources: Resources, content: str, result: str):
        """Method for adding a task to the queue by the publisher

        Parameters:
          All parameters from the Task class except id, which is added by the class
        """
        # inserts a task as task_tuple (heapq), maintaining queue invariance
        task_tuple = (priority, self.__id, resources, content, result)
        heappush(self.queue, task_tuple)
        self.__id += 1

    def get_task(self, available_resources: Resources) -> Task:
        """Method for retrieving a task from queue for a consumer

        Parameters:
          available_resources (Resources): Available resources to complete the task
        Returns:
          (Task): Task for the consumer
        """
        empty_queue = len(self.queue) == 0
        temp_queue = (
            []
        )  # auxiliary list of higher-priority tasks that do not meet the resources requirements
        try:
            # extract first task from the queue
            # heappop - extract task with highest priority, maintaining queue invariance
            task_tuple = heappop(self.queue)
            temp_queue.append(task_tuple)
            resources = task_tuple[2]
            # select the highest priority task that matches resources
            while (
                resources.ram > available_resources.ram
                or resources.cpu_cores > available_resources.cpu_cores
                or resources.gpu_cores > available_resources.gpu_cores
            ):
                task_tuple = heappop(self.queue)
                temp_queue.append(task_tuple)
                resources = task_tuple[2]

            # return tasks from the auxiliary list to the queue
            # add while maintaining order instead of repeated calls to heappush
            for i in reversed(range(len(temp_queue))):
                # do not add a suitable task
                if task_tuple[1] != temp_queue[i][1]:
                    self.queue.insert(0, temp_queue[i])

            # Note: task_tuple = (priority, id, resources, content, result)
            return Task(
                task_tuple[1], task_tuple[0], resources, task_tuple[3], task_tuple[4]
            )

        # We catch the exception if the queue is empty or there are not enough resources for any of the tasks
        except IndexError:
            message = ""

            if empty_queue:
                message = "Task queue is empty"
            else:
                message = "Specified available resources are insufficient for any tasks in queue"
            print(message)
            return None
