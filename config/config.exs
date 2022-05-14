import Config

config :ra,
  logger_module: RaUtil.Logger

config :ra_util,
  log_level: :warn

import_config "#{config_env()}.exs"
