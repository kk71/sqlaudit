# Author: kk.Fang(fkfkbill@gmail.com)

from .base import *


class APIDocHandler(BaseReq):

    def get(self):
        from .modules import url_table
        tb = url_table.get_html_string(attributes={"class": "table table-striped table-hover"})
        s = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css" integrity="sha384-9aIt2nRpC12Uk9gS9baDl411NQApFmC26EwAOH8WgZl5MYYxFfc+NcPb1dKGj7Sk" crossorigin="anonymous">
    <title>SQLAudit API Documentation</title>
</head>
<body>

{tb}

</body>
</html>'''

        self.finish(s)
