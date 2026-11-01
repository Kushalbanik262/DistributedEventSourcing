const levels = { DEBUG: 10, INFO: 20, WARN: 30, ERROR: 40 };
const configuredLevel = (process.env.LOG_LEVEL ?? 'INFO').toUpperCase();
const threshold = levels[configuredLevel] ?? levels.INFO;

function formatDetails(details) {
  if (details === undefined || details === null || details === '') return '';
  if (details instanceof Error) {
    return JSON.stringify({ name: details.name, message: details.message, stack: details.stack });
  }
  if (typeof details === 'string') return details;
  try {
    return JSON.stringify(details, (_key, value) => value instanceof Error
      ? { name: value.name, message: value.message, stack: value.stack, code: value.code }
      : value);
  } catch {
    return String(details);
  }
}

export function log(level, service, operation, details) {
  const normalizedLevel = String(level).toUpperCase();
  if (!(normalizedLevel in levels) || levels[normalizedLevel] < threshold) return;

  const timestamp = new Date().toISOString();
  const additionalInfo = formatDetails(details);
  const line = `[${timestamp}][${normalizedLevel}][${service}][${operation}]${additionalInfo ? ` ${additionalInfo}` : ''}`;

  if (normalizedLevel === 'ERROR') console.error(line);
  else if (normalizedLevel === 'WARN') console.warn(line);
  else console.log(line);
}

export const debug = (service, operation, details) => log('DEBUG', service, operation, details);
export const info = (service, operation, details) => log('INFO', service, operation, details);
export const warn = (service, operation, details) => log('WARN', service, operation, details);
export const error = (service, operation, details) => log('ERROR', service, operation, details);

info('logger', 'configured', { minimumLevel: configuredLevel in levels ? configuredLevel : 'INFO' });
