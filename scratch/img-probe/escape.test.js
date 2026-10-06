// Fixture: a hostile string must come back HTML-escaped.
const escapeHtml = (s) => s.replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);

const hostile = "<img src=x>";
if (escapeHtml(hostile) !== "&#60;img src=x&#62;") throw new Error("not escaped");
