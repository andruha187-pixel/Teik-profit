"""
Изолированный тест: проверяет, вообще проходит ли реальный ордер на твоём
аккаунте, БЕЗ основного бота. Использует тот же polymarket-client SDK, что
и бот — если тут работает, работает и в боте.

Запуск: python -m scripts.test_live_order
Использует .env — заполни его как для основного бота.

Логика теста: ставит market-ордер BUY на 1 USDC с ценовым потолком 0.01
(заведомо ниже любого реального ask) и order_type=FOK — почти наверняка не
исполнится (FOK: либо всё, либо ничего), то есть если дойдёт до ответа
сервера без ошибки авторизации/адреса, конфигурация верна.
"""
from __future__ import annotations
import asyncio
import sys

sys.path.insert(0, ".")
from config import settings  # noqa: E402


async def main():
    if not settings.POLY_PRIVATE_KEY:
        print("POLY_PRIVATE_KEY не задан в .env — нечего тестировать.")
        return

    from polymarket import AsyncSecureClient

    print(f"Хост: {settings.POLY_HOST} | funder/wallet: {settings.POLY_FUNDER_ADDRESS or '(не задан, берётся из ключа)'}")

    kwargs = dict(private_key=settings.POLY_PRIVATE_KEY)
    if settings.POLY_FUNDER_ADDRESS:
        kwargs["wallet"] = settings.POLY_FUNDER_ADDRESS

    print("Создаю клиент (SDK сам определит тип кошелька)...")
    client = await AsyncSecureClient.create(**kwargs)
    print(f"Клиент создан. wallet_type: {getattr(client, 'wallet_type', '?')} | "
          f"wallet: {getattr(client, 'wallet', '?')} | signer: {getattr(client, 'signer', '?')}")

    token_id = input("Вставь token_id любого сейчас активного рынка (up или down токен): ").strip()

    print("Проверяю баланс/аллованс...")
    try:
        balance = await client.get_balance_allowance(asset_type="COLLATERAL")
        print("Баланс/аллованс:", balance)
    except Exception as exc:
        print("⚠️ Не удалось прочитать баланс:", exc)

    print("Отправляю тестовый FOK-ордер на 1 USDC по потолку цены 0.01 (не должен исполниться)...")
    try:
        resp = await client.place_market_order(
            token_id=token_id,
            side="BUY",
            amount="1",
            max_price="0.01",
            order_type="FOK",
        )
        print("✅ Сервер принял запрос без ошибки авторизации/адреса:")
        print(resp)
        print("\nЕсли видишь статус вроде 'unmatched'/'cancelled' без текста про "
              "maker address — конфигурация рабочая, можно пробовать LIVE в боте "
              "с маленьким размером позиции.")
    except Exception as exc:
        print("❌ Ошибка при размещении ордера:")
        print(exc)
        print("\nЕсли текст содержит 'maker address not allowed' — сообщи мне точный "
              "текст ошибки, разберём отдельно; это была бы неожиданность для этого SDK.")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
