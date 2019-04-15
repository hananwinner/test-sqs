import os
import boto3
import signal
import botocore
import time

in_flight_m = None
consecutive_time_outs = 0
queues = []


def _get_q_config():
    _queues = os.environ.get('QUEUES')
    if _queues:
        _queues = [s.strip() for s in ','.split(_queues)]
    else:
        _queues = ['test-sqs-q1', 'test-sqs-q2']
    return _queues


sqs = boto3.resource('sqs')
dead_letter = sqs.get_queue_by_name(QueueName='test-sqs-dead-letter')
event_q = sqs.get_queue_by_name(QueueName='test-event-q')
_qs = _get_q_config()
print("qs from config: {}".format(_qs))
queues = [sqs.get_queue_by_name(QueueName=q) for q in _get_q_config()]


def process_message(m):
    print('processing message..')
    if m.message_attributes is not None:
        attempts = m.message_attributes.get('attempts').get('StringValue')
        print('debug: raw value attemts: {}'.format(attempts))
        if attempts:
            attempts = int(attempts)
        else:
            attempts = 0
        print("debug: attempt: {}".format(attempts))
    print("debug: body: \n{}".format(m.body))

    if m.body == "crash":
        print('simulating crash')
        raise OSError("simulating a crash")
    elif m.body == "timeout":
        print('simulating time out')
        raise TimeoutError("message processing timed out")
    elif m.body == "long":
        print('simulating long meesgae by 20 sec sleep')
        time.sleep(20)


def _resend_with_attr(q, m, message_attempts):
    q.send_message(
        MessageBody=m.body,
        MessageAttributes={
            'attempts': {
                'StringValue': str(message_attempts),
                'DataType': 'Number'
            }
        }
    )


def _process_timed_out_message(q, m):
    result_status = "dled"
    message_attempts = 0
    if m.message_attributes is not None:
        _dict_attempts = m.message_attributes.get('attempts')
        if _dict_attempts is not None:
            message_attempts = _dict_attempts.get('StringValue', 0)
            message_attempts = int(message_attempts)
    message_attempts += 1
    try:
        if message_attempts >= 2:
            _resend_with_attr(dead_letter, m, message_attempts)
        else:
            _resend_with_attr(q, m, message_attempts)
            result_status = "retried"
    finally:
        m.delete()
    return result_status


def nack_message(m):
    print('nacking message {}'.format(m.message_id))
    try:
        m.change_visibility(VisibilityTimeout=0)
    except botocore.exceptions.ClientError:
        pass


def produce_event_message(m, status, _tic=None):
    if _tic is not None:
        _dur = time.time() - _tic
    else:
        _dur = "unknown"
    event_q.send_message(
        MessageBody="message {} {} in {}s\norig body: \n{}".format(m.message_id, status, _dur,  m.body),
        MessageAttributes={
            'duration': {
                'StringValue': str(_dur),
                'DataType': 'String'
            }
        }
    )


def nack_inflight_message():
    if in_flight_m:
        print('nacking inflight message {}'.format(in_flight_m.message_id))
        nack_message(in_flight_m)
        produce_event_message(in_flight_m, "nacked on TERM")
    else:
        print('no inflight messages')


def sigterm_handler(signum, frame):
    print('external termination. \nsignum: {}\nframe: {}'.format(signum, frame))
    nack_inflight_message()


signal.signal(signal.SIGTERM, sigterm_handler)



def main():
    global in_flight_m
    global consecutive_time_outs
    while True:
        try:
            for q in queues:
                for m in q.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=['attempts']):
                    in_flight_m = m
                    _tic = time.time()
                    _status = ""
                    try:
                        process_message(m)
                        in_flight_m = None
                        _status = "processed"
                    except TimeoutError as toe:
                        print(toe)
                        consecutive_time_outs += 1
                        _status = _process_timed_out_message(q, m)
                        in_flight_m = None
                        produce_event_message(m, _status, _tic)
                        raise toe
                    except OSError as ose:  # general engine error
                        print(ose)
                        nack_message(m)
                        in_flight_m = None
                        _status = "nacked"
                        produce_event_message(m, _status, _tic)
                        raise ose
                    else:
                        consecutive_time_outs = 0
                        m.delete()
                        in_flight_m = None
                        _status = "acked"
                        produce_event_message(m, _status, _tic)



        except OSError:
            pass  # by choice we let it hang till we figure out
        except TimeoutError:
            if consecutive_time_outs > 5:
                # die
                print('dying after {} cons timeouts'.format(consecutive_time_outs))
                raise
            else:
                print('sleep to recover after {} cons timeout'.format(consecutive_time_outs))
                time.sleep(consecutive_time_outs)  # let the system recover..


if __name__ == "__main__":
    main()
