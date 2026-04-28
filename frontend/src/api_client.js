export function fetchWithTimeout(input, init, timeoutMs) {
  const controller = new AbortController();
  const externalSignal = init && init.signal;
  if (externalSignal) {
    if (externalSignal.aborted) {
      controller.abort();
    } else {
      externalSignal.addEventListener("abort", function () {
        controller.abort();
      }, { once: true });
    }
  }
  const timer = window.setTimeout(function () {
    controller.abort();
  }, timeoutMs);
  const opts = Object.assign({}, init || {}, { signal: controller.signal });
  return fetch(input, opts).finally(function () {
    window.clearTimeout(timer);
  });
}
