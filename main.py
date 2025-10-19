import json
import asyncio
from datetime import datetime, timedelta
from collections import defaultdict
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import pytz

# === Настройки ===
TIMEZONE = 'Asia/Tashkent'
DAYS_BACK = 7

API_ID = int(input("Введите API_ID: ").strip())
API_HASH = input("Введите API_HASH: ").strip()
GROUP_INPUT = input("Введите username или ID группы (например, @mygroup или 123456789): ").strip()

try:
    GROUP_ID = int(GROUP_INPUT) if GROUP_INPUT.lstrip('-').isdigit() else GROUP_INPUT
except ValueError:
    GROUP_ID = GROUP_INPUT

SESSION_NAME = "thread_analyzer"
tzinfo = pytz.timezone(TIMEZONE)
now = datetime.now(tz=tzinfo)
start_date = now - timedelta(days=DAYS_BACK)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

async def main():
    await client.start()

    try:
        entity = await client.get_entity(GROUP_ID)
    except Exception as e:
        print(f"❌ Ошибка получения группы: {e}")
        return

    if not getattr(entity, 'megagroup', False):
        print("❌ Указанный чат не является супергруппой.")
        return

    # Проверяем, включены ли форумы
    has_forum = getattr(entity, 'forum', False)
    if has_forum:
        print("✅ Форумы включены — анализ по топикам.")
    else:
        print("ℹ️ Форумы отключены — анализ по reply-цепочкам.")

    # Получаем все сообщения за последние 7 дней
    print("📥 Загрузка сообщений за последние 7 дней...")
    all_messages = []
    offset_id = 0
    limit = 100

    while True:
        history = await client(GetHistoryRequest(
            peer=entity,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))

        if not history.messages:
            break

        batch = []
        for msg in history.messages:
            if not msg.date:
                continue
            msg_date = msg.date.astimezone(tzinfo)
            if msg_date < start_date:
                continue  # пропускаем старые
            batch.append(msg)

        if not batch:
            break

        all_messages.extend(batch)
        offset_id = history.messages[-1].id

        # Если самое старое сообщение в пачке уже вне диапазона — выходим
        if history.messages[-1].date.astimezone(tzinfo) < start_date:
            break

    print(f"📥 Загружено {len(all_messages)} сообщений.")

    days_data = defaultdict(list)

    if has_forum:
        # === Режим: форумы (topics) ===
        # Группируем сообщения по top_message_id (ID заголовка топика)
        topics = defaultdict(list)
        for msg in all_messages:
            if msg.reply_to and getattr(msg.reply_to, 'forum_topic', False):
                topic_id = msg.reply_to.reply_to_top_id
                topics[topic_id].append(msg)
            elif hasattr(msg, 'action') and msg.action is None:
                # Это может быть заголовок топика (top_message)
                # В Telethon у заголовка топика нет reply_to, но он создаётся как обычное сообщение
                # и другие сообщения ссылаются на него через reply_to_top_id
                # Поэтому просто добавим его в свой топик
                topics[msg.id].append(msg)

        # Теперь обрабатываем каждый топик
        for topic_id, msgs in topics.items():
            if len(msgs) < 2:
                continue

            # Найдём заголовок топика (первое сообщение с этим ID)
            title_msg = next((m for m in msgs if m.id == topic_id), None)
            topic_title = (title_msg.message if title_msg else "[Без названия]")[:50]

            first_msg = min(msgs, key=lambda m: m.date)
            thread_date = first_msg.date.astimezone(tzinfo).date()

            users = set()
            for m in msgs:
                if m.from_id and hasattr(m.from_id, 'user_id'):
                    users.add(m.from_id.user_id)
                else:
                    users.add('anonymous')

            days_data[thread_date].append({
                "topic": topic_title,
                "messages": len(msgs),
                "users": len(users)
            })

    else:
        # === Режим: reply-цепочки ===
        msg_dict = {msg.id: msg for msg in all_messages}
        threads = defaultdict(list)

        for msg in all_messages:
            if not msg.reply_to or not hasattr(msg.reply_to, 'reply_to_msg_id'):
                threads[msg.id].append(msg)
            else:
                reply_to_id = msg.reply_to.reply_to_msg_id
                # Поднимаемся до корня
                current = reply_to_id
                while current in msg_dict and msg_dict[current].reply_to:
                    current = msg_dict[current].reply_to.reply_to_msg_id
                threads[current].append(msg)

        for root_id, thread_msgs in threads.items():
            if len(thread_msgs) < 2:
                continue

            root_msg = msg_dict.get(root_id)
            if not root_msg:
                continue

            thread_date = root_msg.date.astimezone(tzinfo).date()
            if thread_date < start_date.date():
                continue

            users = set()
            for m in thread_msgs:
                if m.from_id and hasattr(m.from_id, 'user_id'):
                    users.add(m.from_id.user_id)
                else:
                    users.add('anonymous')

            topic_text = (root_msg.message or '').replace('\n', ' ')[:50] or "[Без темы]"
            days_data[thread_date].append({
                "topic": topic_text,
                "messages": len(thread_msgs),
                "users": len(users)
            })

    # Формируем отчёт по всем дням (включая пустые)
    report_days = []
    current = start_date.date()
    end = now.date()
    while current <= end:
        threads_list = days_data.get(current, [])
        report_days.append({
            "date": current.isoformat(),
            "threads": threads_list
        })
        current += timedelta(days=1)

    report = {
        "timezone": TIMEZONE,
        "days": report_days
    }

    with open("report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print("✅ Анализ завершён. Отчёт сохранён в report.json")

if __name__ == "__main__":
    asyncio.run(main())