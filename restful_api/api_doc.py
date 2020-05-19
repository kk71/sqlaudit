# Author: kk.Fang(fkfkbill@gmail.com)

import json

import requests
from schema import Or
from tornado.template import Template

from utils.schema_utils import *
import restful_api.urls
from .base import *


class APIDocHandler(BaseReq):

    def get(self):
        s = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">
    <script src="https://code.jquery.com/jquery-3.5.1.min.js">
    <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.0/dist/umd/popper.min.js" integrity="sha384-Q6E9RHvbIyZFJoft+2mJbHaEWldlvI9IOYy5n3zV9zzTtmI3UksdQRVvoxMfooAo" crossorigin="anonymous"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/js/bootstrap.min.js" integrity="sha384-OgVRvuATP1z7JjHLkuOU7Xw704+h835Lr+6QL9UvYjZE3Ipu6Tp75j7Bh/kR0JKI" crossorigin="anonymous"></script>
    <script src="https://cdn.bootcss.com/blueimp-md5/2.10.0/js/md5.js"></script>
    <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/highlight.js/10.0.3/styles/default.min.css">
    <script src="//cdnjs.cloudflare.com/ajax/libs/highlight.js/10.0.3/highlight.min.js"></script>
    
    <title>SQLAudit API Documentation</title>
    
<script>
var token = "";
function get_token() {
    $.ajax({
        type: "POST",
        url: "/api/auth/auth/login",
        async: false,
        data: JSON.stringify({
            "login_user": $("#login_user").val(),
            "password": md5($("#password").val())
        }),
        dataType: "json",
        contentType: "application/json; charset=utf-8", 
        success: function(s) {
            $("#token").text(s.content.token);
            token = s.content.token;
            $("#tokenModal").modal("hide");
        }
    });
}

function show_req_modal(the_id) {
    $("#reqModel").modal("show");
    $.ajax({
        type: "POST",
        url: "/apidoc", 
        async: false,
        data: JSON.stringify({request_id: the_id}),
        dataType: "json",
        contentType: "application/json; charset=utf-8", 
        success: function(s){
            $("#argument").text(s.content.argument);
            $("#reqInfo").val(s.content.method + ": " + s.content.url);
            $("#docString").val(s.content.docstring);
        }
    });
}
</script>
</head>
<body class="container-fluid">


<div class="modal fade" id="tokenModal" tabindex="-1" role="dialog" aria-labelledby="exampleModalLabel" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title" id="exampleModalLabel">get token</h5>
        <button type="button" class="close" data-dismiss="modal" aria-label="Close">
          <span aria-hidden="true">&times;</span>
        </button>
      </div>
      <div class="modal-body">
      
<form>
  <div class="form-group">
    <label for="login_user">login_user</label>
    <input type="text" class="form-control" id="login_user">
  </div>
  <div class="form-group">
    <label for="password">password(plain password, not md5 encoded)</label>
    <input type="text" class="form-control" id="password">
  </div>
</form>
      
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-dismiss="modal">close</button>
        <button onclick="get_token()" type="button" class="btn btn-primary">get token</button>
      </div>
    </div>
  </div>
</div>

<button type="button" class="btn btn-primary" data-toggle="modal" data-target="#tokenModal">get token</button>
<div id="token"></div>


<div class="modal fade" id="reqModel" tabindex="-1" role="dialog" aria-labelledby="exampleModalLabel" aria-hidden="true">
  <div class="modal-dialog modal-xl">
    <div class="modal-content">
      <div class="modal-header">
        <h5 class="modal-title" id="exampleModalLabel">test for RESTful API</h5>
        <button type="button" class="close" data-dismiss="modal" aria-label="Close">
          <span aria-hidden="true">&times;</span>
        </button>
      </div>
      <div class="modal-body">
      
      <form>
          <div class="form-group row">
            <label for="reqInfo" class="col-sm-2 col-form-label">request</label>
            <div class="col-sm-10">
              <input type="text" readonly class="form-control-plaintext" id="reqInfo" value="">
            </div>
          </div>
          <div class="form-group row">
            <label for="docString" class="col-sm-2 col-form-label">usage</label>
            <div class="col-sm-10">
              <input type="text" readonly class="form-control-plaintext" id="docString" value="">
            </div>
          </div>
          <div class="form-group">
            <label for="argument">input arguments</label>
            <textarea class="form-control" id="argument" rows="9"></textarea>
          </div>
          <div class="form-group">
            <label for="resp">response</label>
            <pre><code class="json" id="resp"></code></pre>
            
          </div>
      </form>
      
      </div>
      <div class="modal-footer">
        <button type="button" class="btn btn-secondary" data-dismiss="modal">close</button>
        <button type="button" class="btn btn-primary">send</button>
      </div>
    </div>
  </div>
</div>

<table class="table table-striped table-hover">
    <tr>
        <th>group</th>
        <th>URL</th>
        <th>request methods</th>
        <th>usage</th>
        <th>argument</th>
    </tr>

    {% for group_name, reqs in verbose_structured_urls.items() %}
    {% for url, method, docstring, argument, the_id, req_object in reqs %}
    <tr>
        <td>{{group_name}}</td>
        <td>{{url}}</td>
        <td>{{method}}</td>
        <td>{{docstring}}</td>
        <td>
            <a href="javascript: show_req_modal('{{the_id}}')"><button type="button" class="btn btn-primary">
            send
            </button></a>
        </td>
    </tr>
    {% end %}
    {% end %}

</table>

</body>
</html>'''
        page_str = Template(s).generate(
            verbose_structured_urls=restful_api.urls.verbose_structured_urls
        )
        self.finish(page_str)

    def post(self):
        """获取请求信息"""
        params = self.get_json_args(Schema({
            "request_id": scm_unempty_str,
        }))
        request_id = params["request_id"]
        for group_name, reqs in restful_api.urls.verbose_structured_urls.items():
            for req in reqs:
                if req[-2] == request_id:
                    return self.resp({
                        "url": req[0],
                        "method": req[1],
                        "docstring": req[2],
                        "argument": json.dumps(req[3], indent=4),
                        "the_id": req[4]
                    })
        self.resp_not_found()


class APIDocTestHandler(BaseReq):

    def post(self):
        params = self.get_query_args(Schema({
            "request_id": scm_unempty_str,
            "argument": {
                "querystring": {
                    scm_optional(str): str
                },
                scm_optional("json"): Or(list, dict),
                "header": {
                    scm_optional("token"): scm_str
                }
            }
        }))

