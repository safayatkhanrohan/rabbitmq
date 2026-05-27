// Structured JSON logs so every line is greppable and carries the correlation
// id that threads a single order across all services.
function emit(level, service, message, meta) {
  process.stdout.write(
    JSON.stringify({
      ts: new Date().toISOString(),
      level,
      service,
      message,
      ...meta,
    }) + '\n'
  );
}

module.exports = function createLogger(service) {
  return {
    info: (message, meta = {}) => emit('info', service, message, meta),
    warn: (message, meta = {}) => emit('warn', service, message, meta),
    error: (message, meta = {}) => emit('error', service, message, meta),
  };
};
