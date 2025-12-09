export const getBackendUrl = () => {
  const isBrowser = typeof window !== "undefined";
  const protocol =
    process.env.NEXT_PUBLIC_BACKEND_HTTPS === "true"
      ? "https"
      : isBrowser && window.location.protocol === "https:"
        ? "https"
        : "http";
  const host =
    process.env.NEXT_PUBLIC_BACKEND_HOST ||
    (isBrowser ? window.location.hostname : "localhost");
  const port =
    process.env.NEXT_PUBLIC_BACKEND_PORT ||
    (isBrowser ? window.location.port || "8081" : "8081");

  return port ? `${protocol}://${host}:${port}` : `${protocol}://${host}`;
};
