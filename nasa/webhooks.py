#%%
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    # Enviar notificação para o Slack
    slack_webhook_url = 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
    message = f"Nova tarefa criada: {data['task_name']} com a descrição: {data['task_description']}"
    requests.post(slack_webhook_url, json={'text': message})
    return jsonify(status='success'), 200

if __name__ == '__main__':
    app.run(port=5000)