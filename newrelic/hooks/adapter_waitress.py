import newrelic.api.web_transaction
import newrelic.api.in_function

def instrument_waitress_server(module):

    def wrap_wsgi_application_entry_point(server, application,
                                          *args, **kwargs):
        application = newrelic.api.web_transaction.WSGIApplicationWrapper(
                application)
        args = [server, application] + list(args)
        return (args, kwargs)

    newrelic.api.in_function.wrap_in_function(module,
            'WSGIServer.__init__', wrap_wsgi_application_entry_point)
