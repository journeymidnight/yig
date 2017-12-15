##JAVASCRIPT访问bucket usage方法

```var jwt = require('jsonwebtoken');
var token = jwt.sign({ bucket: 'perf-test' }, 'secret');
var request = require("request");
request({
  uri: "http://10.71.84.22:9000/admin/usage",
  method: "GET",
  headers: {
    'Authorization': 'Bearer ' + token
  },
  timeout: 10000,
  followRedirect: true,
  maxRedirects: 1
}, function(error, response, body) {
  console.log(body);
});
