import requests
import logging
logger = logging.getLogger('airflow.task')

class TelegramNotifier:
    def __init__(self, chat_id, bot_token):
        self.chat_id = chat_id
        self.bot_token = bot_token
        self.url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"

    def notify(self, context):
        message = '[ERROR] {} {} \n {}'.format(context['dag'], context['task'], str(context.get('exception')))
        payload = {'chat_id': self.chat_id, 'text': message}

        response = requests.post(self.url, data=payload)
        return response.json()
