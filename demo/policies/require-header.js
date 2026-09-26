// A custom policy: reject requests missing a header. Referenced from YAML as `require-header`.
module.exports = {
  name: 'require-header',
  priority: 10, // higher runs first
  schema: { type: 'object', required: ['header'], properties: { header: { type: 'string' } } },
  create: ({ header }) => (req, res, next) =>
    req.headers[header.toLowerCase()] ? next() : res.status(400).json({ message: `missing ${header} header`, response_code: 'DEMO400' }),
};
