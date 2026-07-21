from notification_service.core.logger import get_logger


logger = get_logger(__name__)


class ProcessNotification:
    def __init__(self):
        ...

    '''
    читает сообщение из notification.tg.  
    База данных подтверждает, что сообщение еще не обрабатывалось.
    Воркер пытается отправить его в Telegram.
    Ошибка сети? Включается Exponential Backoff. Ждем 1с, 2с, 4с... 
    Пробуем снова.Все попытки сгорели? Отправляем сообщение как есть в notification.tg.dlq.
    Делаем commit() в основном топике, берем следующее сообщение.
    '''

