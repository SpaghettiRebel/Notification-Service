topics_names = ['sms', 'tg', 'sms']

group_prefix = 'notification'


def get_topics():
    return [f'{group_prefix}.{i}' for i in topics_names]
