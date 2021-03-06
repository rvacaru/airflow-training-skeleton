from airflow.hooks.http_hook import HttpHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.operators.http_operator import SimpleHttpOperator

class RazHttpOperator(BaseOperator):

    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the sensor against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url
    :type endpoint: string
    :param method: The HTTP method to use, default = "POST"
    :type method: string
    :param data: The data to pass. POST-data in POST/PUT and params
        in the URL for a GET request.
    :type data: For POST/PUT, depends on the content-type parameter,
        for GET a dictionary of key/value string pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        Returns True for 'pass' and False otherwise.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    """

    template_fields = ('endpoint', 'data',)
    template_ext = ()
    ui_color = '#f4a460'

    @apply_defaults
    def __init__(self,
                 endpoint,
                 method='POST',
                 data=None,
                 headers=None,
                 response_check=None,
                 extra_options=None,
                 xcom_push=False,
                 http_conn_id='http_default', *args, **kwargs):
        """
        If xcom_push is True, response of an HTTP request will also
        be pushed to an XCom.
        """
        super(SimpleHttpOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.data = data or {}
        self.response_check = response_check
        self.extra_options = extra_options or {}
        self.xcom_push_flag = xcom_push

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        self.log.info("Calling HTTP method")

        response = []
        try:
            response = http.run(self.endpoint,
                                self.data,
                                self.headers,
                                self.extra_options)

        except AirflowException as err:
            if hasattr(err, 'message'):
                print(err.message)
                if err.message.startswith("404"):
                    raise AirflowSkipException("Skip because of 404 error")
            else:
                raise err


        if self.response_check:
            if not self.response_check(response):
                raise AirflowException("Response check returned False.")
        if self.xcom_push_flag:
            return response.text