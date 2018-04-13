from multiprocessing import Process, Queue
import time
import unittest

from shared.constants import *
from messages import *
from edit_distance_worker import EditDistanceWorker

class TestEditDistanceWorker(unittest.TestCase):

    def test_edit_distance_worker(self):
        # create the edit distance worker
        my_id = 12345
        worker_in_queue = Queue()
        worker_out_queue = Queue()
        worker = EditDistanceWorker(my_id, worker_in_queue, worker_out_queue)
        worker_process = Process(target=worker.run)
        worker_process.start()
        time.sleep(1)
        # put the process in active_worker mode 
        worker_in_queue.put(internal_mode_switch_to_worker_message())
        # send the Work Queue Ready message and expect a work request
        worker_in_queue.put(work_queue_ready_message())
        time.sleep(1)
        work_request_msg = worker_out_queue.get_nowait()
        self.assertEqual(work_request_msg[MESSAGE_TYPE], WORK_REQUEST)
        # send a Work Response message with some edit distance test data
        requester_id = work_request_msg[REQUESTER_ID]
        string_pair_id = "abcd"
        test_str_4a = "INTENTION"
        test_str_4b = "EXECUTION"
        edit_distance4_expected = 5
        message_id = "a long identifier created by AWS SQS"
        receipt_handle = "an even longer identifier needed by AWS SQS to delete the queue message once we are done with it"
        work_response_msg = work_response_message(requester_id, string_pair_id, string_a, string_b, message_id, receipt_handle)
        worker_in_queue.put(work_response_msg)
        time.sleep(1)
        # receive a Work Result message and compare the result
        work_result_msg = worker_out_queue.get_nowait()
        self.assertEqual(work_result_msg[MESSAGE_TYPE], WORK_RESULT)
        self.assertEqual(work_result_msg[REQUESTER_ID], requester_id)
        self.assertEqual(work_result_msg[STRING_PAIR_ID], string_pair_id)
        self.assertEqual(work_result_msg[EDIT_DISTANCE], edit_distance4_expected)
        self.assertEqual(work_result_msg[MESSAGE_ID], message_id)
        self.assertEqual(work_result_msg[RECEIPT_HANDLE], receipt_handle)
        # send a Work Result Received message
        work_result_received_msg = work_result_received_message(requester_id, string_pair_id)
        worker_in_queue.put(work_result_received_msg)
        time.sleep(1)
        work_request_msg = worker_out_queue.get_nowait()
        self.assertEqual(work_request_msg[MESSAGE_TYPE], WORK_REQUEST)
        # send a shutdown message and observe shutdown
        worker_in_queue.put(shutdown_message())
        time.sleep(1)
        worker_process.join()

