# -*- coding: utf-8 -*-

import traceback

import yagmail


def send_mail(title, contents, receivers, server_data, path=None, filename=None):
    print(f"Mail parameters: title: {title}, contents: {contents}, receivers: {receivers}, path: {path}, filename: {filename}, server_data: {server_data}")
    mail_host = server_data['mail_server_name']
    mail_port = server_data['port']
    mail_user = server_data['username']
    mail_pass = server_data['password']
    use_ssl = True if server_data['usessl'] else False
    smtp_skip_login = True if not mail_pass else False
    # contents = [contents]
    # data = {
    #     'to': receivers,
    #     'subject': title,
    #     'contents': contents,
    # }
    # if path and filename:
    #     data['attachments'] = {path: filename}
    result = True
    errors = ''
    try:
        yag = yagmail.SMTP(user=mail_user, password=mail_pass, host=mail_host, port=mail_port, smtp_ssl=use_ssl, smtp_skip_login=smtp_skip_login)
        # yag.send(**data)
        yag.send(to=receivers,subject=title,contents=contents,attachments=path)
    except Exception as error:
        print(traceback.format_exc())
        result = False
        errors = error.__str__()
        errors = str(errors)
    print(result, errors)
    return result, errors


if __name__ == '__main__':
    send_mail(title='测试邮件', contents='您好 这是一封测试邮件', receivers='574691837@qq.com', server_data=dict(mail_server_name='smtp.qq.com', port=25, username='1002751472@qq.com', password='ftvcpmagklagbeii', usessl=0), path='/home/sqlaudit/sqlauditcg/sqlaudit2/downloads/mail_files/25测试2802201907011030.zip', filename="SQL审核报告.zip")
