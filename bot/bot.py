from aiogram import Bot, Dispatcher
from aiogram.types import Message, BotCommand
from aiogram.filters import Command
import asyncio
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_ID = -1002314756962  # Replace with your group ID

# Initialize the bot and dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Variables to store the received data and status
received_data = None  # Initially no data
waiting_for_new_data = False  # Status to track if the bot is waiting for new data


@dp.message(Command("start"))
async def start_command(message: Message):
    """Handle /start command."""
    if message.chat.id != GROUP_ID:
        await message.reply("Botdan faqat guruhda foydalanish mumkin.")
        return

    await message.reply(
        "Kerakli buyruqni tanlang:\n"
        "/current_currency - Bugungi kursni ko'rish\n"
        "/update_currency - Kursni o'zgartirish"
    )


@dp.message(Command("current_currency"))
async def current_currency_command(message: Message):
    """Handle /current_currency command."""
    if message.chat.id != GROUP_ID:
        await message.reply("This bot works only in a specific group.")
        return

    global received_data
    if received_data:
        await message.reply(f"Bugungi kurs: {received_data}")
    else:
        await message.reply("Kurs ma'lumotlari mavjud emas.")


@dp.message(Command("update_currency"))
async def update_currency_command(message: Message):
    """Handle /update_currency command."""
    if message.chat.id != GROUP_ID:
        await message.reply("This bot works only in a specific group.")
        return

    global received_data, waiting_for_new_data
    if received_data:
        await message.reply(f"Bugungi kurs: {received_data}. Yangi ma'lumotlarni kiritishingiz mumkin.")
    else:
        await message.reply("Kurs ma'lumotlari yo'q. Yangi ma'lumotlarni kiritishingiz mumkin.")

    # Set the status to wait for new data
    waiting_for_new_data = True
    await message.reply("Yangi kurs ma'lumotlarini yuboring:")


@dp.message(lambda message: message.chat.id == GROUP_ID)
async def receive_new_data(message: Message):
    """Handle receiving new data after update_currency command."""
    global received_data, waiting_for_new_data

    if waiting_for_new_data:
        # Save the new data and reset the status
        received_data = message.text
        waiting_for_new_data = False
        await message.reply(f"Yangi kurs ma'lumotlari saqlandi: {received_data}")
    elif message.reply_to_message is not None:
        # Handle generic replies (fallback logic if needed)
        await message.reply("Sizning xabaringiz qabul qilindi, lekin buyruq kutilayotgan ma'lumot uchun emas.")


async def set_bot_commands():
    """Set bot commands with descriptions."""
    commands = [
        BotCommand(command="start", description="Botni qayta ishga tushirish"),
        BotCommand(command="current_currency", description="Bugungi kursni ko'rish"),
        BotCommand(command="update_currency", description="Kurs ma'lumotlarini yangilash"),
    ]
    await bot.set_my_commands(commands)


async def main():
    """Run the bot."""
    print("Bot is running...")
    await set_bot_commands()  # Register commands
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())