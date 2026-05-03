
curl -s "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates" \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
results = data.get('result', [])
if not results:
    print('No messages found. Send a message to your bot first, then re-run.')
else:
    for r in results:
        chat = r.get('message', {}).get('chat', {})
        print(f\"chat_id: {chat.get('id')}   name: {chat.get('first_name')} {chat.get('username','')}\")
"
