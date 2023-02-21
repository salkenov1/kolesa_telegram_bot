import toml

from pathlib import Path

__dir__ = str(Path(__file__).parent.absolute())
__config_path__ = f"{__dir__}/conf.toml"

config = toml.load(__config_path__)

POSTGRES_URL = f"postgres://{config['postgres']['user']}:{config['postgres']['password']}@{config['postgres']['host']}:{config['postgres']['port']}/{config['postgres']['db']}"

BOT_TOKEN = config['telegram_bot']['token']