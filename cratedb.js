/*
 * Licensed to Crate.io Inc. or its affiliates ("Crate.io") under one or
 * more contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Crate.io licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * However, if you have executed another commercial license agreement with
 * Crate.io these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

var RED = require(process.env.NODE_RED_HOME + "/red/red");
var pg = require('pg');
var named = require('node-postgres-named');
var querystring = require('querystring');

RED.httpAdmin.get('/cratedb/:id', function(req, res) {
  var credentials = RED.nodes.getCredentials(req.params.id);
  if (credentials) {
    res.send(JSON.stringify({
      user: credentials.user,
      hasPassword: (credentials.password && credentials.password != "")
    }));
  } else {
    res.send(JSON.stringify({}));
  }
});

RED.httpAdmin.delete('/cratedb/:id', function(req, res) {
  RED.nodes.deleteCredentials(req.params.id);
  res.send(200);
});

RED.httpAdmin.post('/cratedb/:id', function(req, res) {
  var body = "";
  req.on('data', function(chunk) {
    body += chunk;
  });
  req.on('end', function() {
    var newCreds = querystring.parse(body);
    var credentials = RED.nodes.getCredentials(req.params.id) || {};
    if (newCreds.user == null || newCreds.user == "") {
      delete credentials.user;
    } else {
      credentials.user = newCreds.user;
    }
    if (newCreds.password == "") {
      delete credentials.password;
    } else {
      credentials.password = newCreds.password || credentials.password;
    }
    RED.nodes.addCredentials(req.params.id, credentials);
    res.send(200);
  });
});


function CrateDbNode(n) {
  RED.nodes.createNode(this, n);
  this.hostname = n.hostname;
  this.port = n.port;
  this.db = n.db;
  this.ssl = n.ssl;
}

RED.nodes.registerType("cratedb", CrateDbNode, {
  credentials: {
    user: {
      type: "text"
    },
    password: {
      type: "password"
    }
  }
});

function CrateDbNode(n) {
  RED.nodes.createNode(this, n);

  var node = this;

  node.topic = n.topic;
  node.crate = n.crate;
  node.crateDbConfig = RED.nodes.getNode(this.cratedb);
  node.sqlquery = n.sqlquery;
  node.output = n.output;

  if (node.crateDbConfig) {

    var connectionConfig = {
      host: node.crateDbConfig.hostname,
      port: node.crateDbConfig.port,
      database: node.crateDbConfig.db,
      ssl: node.crateDbConfig.ssl
    };

    var handleError = function(err, msg) {
      node.error(err);
      console.log(err);
      console.log(msg.payload);
      console.log(msg.queryParameters);
    };

    node.on('input', function(msg) {
      pg.connect(connectionConfig, function(err, client, done) {
        if (err) {
          handleError(err, msg);
        } else {
          named.patch(client);

          if (!!!msg.queryParameters)
            msg.queryParameters = {};

          client.query(
            msg.payload,
            msg.queryParameters,
            function(err, results) {
              done();
              if (err) {
                handleError(err, msg);
              } else {
                if (node.output) {
                  msg.payload = results.rows;
                  node.send(msg);
                }
              }
            }
          );
        }
      });
    });
  } else {
    this.error("missing cratedb configuration");
  }

  this.on("close", function() {
    if (node.clientdb) node.clientdb.end();
  });
}

RED.nodes.registerType("cratedb", CrateDbNode);
