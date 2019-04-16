import os
import boto3
import signal
import botocore
import time

in_flight_m = None
consecutive_time_outs = 0


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


def _parse_attribute(m, attr_name, attr_type='StringValue', default_value=None, _cast=int):
    _value = default_value
    if m.message_attributes is not None:
        _attr = m.message_attributes.get(attr_name)
        if _attr:
            _attr_value = _attr.get(attr_type)
            if _attr_value:
                _value = _cast(_attr_value)
    return _value


def _try_ack(m):
    m.delete()


def ack_message(m):
    print('ACK message {}'.format(m.message_id))
    try:
        _try_ack(m)
    except botocore.exceptions.ClientError as ex:
        print("failed to ack message. future duplication is possible.\nex:\n{}".format(ex))


def nack_message(m):
    print('NACK message {}'.format(m.message_id))
    try:
        m.change_visibility(VisibilityTimeout=0)
    except botocore.exceptions.ClientError as ex:
        print('failed to nack message\nbotocore exception:\n {}'.format(ex))


def log_event(m, status, _tic=None):
    print('logging event {} for message {}'.format(status, m.message_id))
    if _tic is not None:
        _dur = time.time() - _tic
    else:
        _dur = "unknown"
    try:
        event_q.send_message(
            MessageBody="message {} {} in {}s\norig body: \n{}".format(m.message_id, status, _dur,  m.body),
            MessageAttributes={
                'duration': {
                    'StringValue': str(_dur),
                    'DataType': 'String'
                }
            }
        )
    except botocore.exceptions.ClientError as ex:
        print('failed log to event queue\nbotocore exception:\n {}'.format(ex))


def nack_inflight_message():
    if in_flight_m is not None:
        print('nacking inflight message {}'.format(in_flight_m.message_id))
        nack_message(in_flight_m)
        log_event(in_flight_m, "nacked on TERM")
    else:
        print('no inflight messages')


def sigterm_handler(signum, frame):
    print('external termination. \nsignum: {}\nframe: {}'.format(signum, frame))
    nack_inflight_message()
    exit(0)


signal.signal(signal.SIGTERM, sigterm_handler)


def try_process_message(m):
    print('processing message..')

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

    _try_ack(m)


def _try_resend_with_attr(q, m, num_attempts):
    q.send_message(
        MessageBody=m.body,
        MessageAttributes={
            'attempts': {
                'StringValue': str(num_attempts),
                'DataType': 'Number'
            }
        }
    )


def _process_timed_out_message(q, m, previous_attempts):
    result_status = "failed to process on timeout"
    _attempts = previous_attempts + 1
    if _attempts >= 2:
        try:
            print("too many attempts. sending to dead-letter.")
            _try_resend_with_attr(dead_letter, m, _attempts)
            result_status = "dead-lettered"
        except botocore.exceptions.ClientError as ex:
            print('failed to send to dead letter\nbotocore exception:\n {}'.format(ex))
            nack_message(m)
        else:
            ack_message(m)
    else:
        try:
            print("resending message with increased attempts counter")
            _try_resend_with_attr(q, m, _attempts)
            result_status = "retried"
        except botocore.exceptions.ClientError as ex:
            print('failed to increase attempts time out attribute by re-sending.\nbotocore exception:\n {}'.format(ex))
            nack_message(m)
        else:
            ack_message(m)

    return result_status


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
                    m_timeout_previous_attempts = _parse_attribute(m, 'attempts', default_value=0)
                    _exception = None
                    try:
                        try_process_message(m)
                        _status = "acked"
                        consecutive_time_outs = 0
                    except TimeoutError as ex:
                        consecutive_time_outs += 1
                        _status = _process_timed_out_message(q, m, m_timeout_previous_attempts)
                        _exception = ex
                    except OSError as ex:  # general engine error
                        nack_message(m)
                        _status = "nacked"
                        _exception = ex

                    in_flight_m = None
                    log_event(m, _status, _tic)
                    if _exception:
                        raise _exception

        except TimeoutError:
            if consecutive_time_outs > 5:
                # die
                print('dying after {} cons timeouts'.format(consecutive_time_outs))
                raise
            else:
                print('sleep to recover after {} cons timeout'.format(consecutive_time_outs))
                time.sleep(consecutive_time_outs)  # let the system recover..

        except OSError as ex:
            print('exception:\n {}'.format(ex))
            # by choice we let it hang till we figure out


if __name__ == "__main__":
    main()
