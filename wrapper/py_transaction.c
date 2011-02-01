/* ------------------------------------------------------------------------- */

/* (C) Copyright 2010-2011 New Relic Inc. All rights reserved. */

/* ------------------------------------------------------------------------- */

#include "py_transaction.h"

#include "py_params.h"
#include "py_traceback.h"

#if 0
#include "py_database_trace.h"
#include "py_external_trace.h"
#include "py_function_trace.h"
#include "py_memcache_trace.h"
#endif

#include "globals.h"
#include "logging.h"

#include "application.h"
#include "genericobject.h"
#include "harvest.h"
#include "web_transaction.h"

#include "pythread.h"

/* ------------------------------------------------------------------------- */

static int NRTransaction_tls_key = 0;

/* ------------------------------------------------------------------------- */

PyObject *NRTransaction_CurrentTransaction()
{
    PyObject *result = NULL;

    if (!NRTransaction_tls_key)
        return NULL;

    result = (PyObject *)PyThread_get_key_value(NRTransaction_tls_key);

    return result;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_new(PyTypeObject *type, PyObject *args,
                                   PyObject *kwds)
{
    NRTransactionObject *self;

    /*
     * Initialise thread local storage if necessary. Do this
     * here rather than init method as technically the latter
     * may not be called.
     */

    if (!NRTransaction_tls_key)
        NRTransaction_tls_key = PyThread_create_key();

    /*
     * Allocate the transaction object and initialise it as per
     * normal.
     */

    self = (NRTransactionObject *)type->tp_alloc(type, 0);

    if (!self)
        return NULL;

    self->application = NULL;
    self->transaction = NULL;
    self->transaction_errors = NULL;

    self->transaction_state = NR_TRANSACTION_STATE_PENDING;

    self->request_parameters = PyDict_New();
    self->custom_parameters = PyDict_New();

    return (PyObject *)self;
}

/* ------------------------------------------------------------------------- */

static int NRTransaction_init(NRTransactionObject *self, PyObject *args,
                              PyObject *kwds)
{
    NRApplicationObject *application = NULL;

    static char *kwlist[] = { "application", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!:Transaction",
                                     kwlist, &NRApplication_Type,
                                     &application)) {
        return -1;
    }

    /*
     * Validate that this method hasn't been called previously.
     */

    if (self->application) {
        PyErr_SetString(PyExc_TypeError, "transaction already initialized");
        return -1;
    }

    /*
     * Cache reference to the application object instance as we
     * will need that latter when doing some work arounds for
     * lack of thread safety in agent library client code. The
     * application object also holds the enabled flag for the
     * application. If the application isn't enabled then we
     * don't actually do anything but still create objects as
     * standins so any code still runs.
     */

    self->application = application;
    Py_INCREF(self->application);

    if (application->enabled) {
        self->transaction = nr_web_transaction__allocate();

        self->transaction->path_type = NR_PATH_TYPE_UNKNOWN;
        self->transaction->path = NULL;
        self->transaction->realpath = NULL;
    }

    return 0;
}

/* ------------------------------------------------------------------------- */

static void NRTransaction_dealloc(NRTransactionObject *self)
{
    /*
     * If transaction still running when this object is being
     * destroyed then force call of exit method to finalise the
     * transaction.
     */

    if (self->transaction_state == NR_TRANSACTION_STATE_RUNNING) {
        PyObject *result;

        result = PyObject_CallMethod((PyObject *)self, "__exit__", "(OOO)",
                                     Py_None, Py_None, Py_None);

        Py_XDECREF(result);
    }

    Py_DECREF(self->custom_parameters);
    Py_DECREF(self->request_parameters);

    Py_XDECREF(self->application);

    Py_TYPE(self)->tp_free(self);
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_enter(NRTransactionObject *self,
                                     PyObject *args)
{
    nr_node_header *save;

    if (!self->application) {
        PyErr_SetString(PyExc_TypeError, "transaction not initialized");
        return NULL;
    }

    if (self->transaction_state == NR_TRANSACTION_STATE_STOPPED) {
        PyErr_SetString(PyExc_RuntimeError, "transaction already completed");
        return NULL;
    }

    if (self->transaction_state == NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction already active");
        return NULL;
    }

    self->transaction_state = NR_TRANSACTION_STATE_RUNNING;

    /*
     * Save away the current transaction object into thread
     * local storage so that can easily access the current
     * transaction later on when creating traces without the
     * need to have a handle to the original transaction.
     */

    PyThread_set_key_value(NRTransaction_tls_key, self);

    /*
     * If application was not enabled and so we are running
     * as a dummy transaction then return without actually
     * doing anything.
     */

    if (!self->transaction) {
        Py_INCREF(self);
        return (PyObject *)self;
    }

    /*
     * Start timing for the current transaction.
     */

    nr_node_header__record_starttime_and_push_current(
            (nr_node_header *)self->transaction, &save);

    Py_INCREF(self);
    return (PyObject *)self;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_exit(NRTransactionObject *self,
                                    PyObject *args)
{
    int keep_wt = 0;

    nrapp_t *application;

    PyObject *type = NULL;
    PyObject *value = NULL;
    PyObject *traceback = NULL;

    if (!PyArg_ParseTuple(args, "OOO:__exit__", &type, &value, &traceback))
        return NULL;

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    /*
     * Remove the reference to the transaction from thread
     * local storage.
     */

    PyThread_delete_key_value(NRTransaction_tls_key);

    /*
     * If application was not enabled and so we are running
     * as a dummy transaction then return without actually
     * doing anything.
     */

    if (!self->transaction) {
        self->transaction_state = NR_TRANSACTION_STATE_STOPPED;

        Py_INCREF(Py_None);
        return Py_None;
    }

    /*
     * Treat any exception passed in as being unhandled and
     * record details of exception against the transaction.
     * It is presumed that original error was not registered in
     * this context and so do not need to restore it when any
     * error here is cleared.
     */

    if (type != Py_None && value != Py_None && traceback != Py_None) {
        PyObject *result;

        result = PyObject_CallMethod((PyObject *)self, "runtime_error",
                                     "(OOO)", type, value, traceback);

        Py_XDECREF(result);
    }

    nr_node_header__record_stoptime_and_pop_current(
            (nr_node_header *)self->transaction, NULL);

    pthread_mutex_lock(&(nr_per_process_globals.harvest_data_mutex));

    application = self->application->application;

    /*
     * TODO Switching what the current application is here is a
     * PITA. The following harvest function should accept the
     * application as a parameter rather than internally
     * consulting the global variable referencing the current
     * application. See more details on Pivotal Tracker at
     * https://www.pivotaltracker.com/projects/154789.
     */

    nr__switch_to_application(application);

    keep_wt = nr__distill_web_transaction_into_harvest_data(
            self->transaction);

    /*
     * Only add request parameters and custom parameters into
     * web transaction object if the record is being kept due to
     * associated errors or because it is being tracked as a
     * slow transaction.
     */

    if (keep_wt || self->transaction_errors != NULL) {
        nrpy__merge_dict_into_params_at(self->transaction->params,
                                        "request_parameters",
                                        self->request_parameters);
        nrpy__merge_dict_into_params_at(self->transaction->params,
                                        "custom_parameters",
                                        self->custom_parameters);
    }

    /*
     * Process any errors associated with transaction and destroy
     * the transaction. Errors can be from unhandled exception
     * that propogated all the way back up the call stack, or one
     * which was explicitly attached to transaction by user code
     * but then supressed within the code.
     */

    nr_transaction_error__process_errors(self->transaction_errors,
            application->pending_harvest->metrics);

    if (!keep_wt)
        nr_web_transaction__destroy(self->transaction);

    nr__merge_errors_from_to(&self->transaction_errors,
                             &application->pending_harvest->errors);

    pthread_mutex_unlock(&(nr_per_process_globals.harvest_data_mutex));

    self->transaction_state = NR_TRANSACTION_STATE_STOPPED;

    Py_INCREF(Py_None);
    return Py_None;
}

/* ------------------------------------------------------------------------- */

#if 0
static PyObject *NRTransaction_function_trace(
        NRTransactionObject *self, PyObject *args)
{
    NRFunctionTraceObject *rv;

    const char *funcname = NULL;
    const char *classname = NULL;
    const char *scope = NULL;

    if (!PyArg_ParseTuple(args, "s|zz:function_trace", &funcname,
              &classname, &scope)) {
        return NULL;
    }

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    if (self->transaction) {
        rv = NRFunctionTrace_New(self->transaction, funcname,
                classname, scope);
    }
    else
        rv = NRFunctionTrace_New(NULL, NULL, NULL, NULL);

    if (rv == NULL)
        return NULL;

    return (PyObject *)rv;
}

static PyObject *NRTransaction_external_trace(
        NRTransactionObject *self, PyObject *args)
{
    NRExternalTraceObject *rv;

    const char *url = NULL;

    if (!PyArg_ParseTuple(args, "s:external_trace", &url))
        return NULL;

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    if (self->transaction)
        rv = NRExternalTrace_New(self->transaction, url);
    else
        rv = NRExternalTrace_New(NULL, NULL);

    if (rv == NULL)
        return NULL;

    return (PyObject *)rv;
}

static PyObject *NRTransaction_memcache_trace(
        NRTransactionObject *self, PyObject *args)
{
    NRMemcacheTraceObject *rv;

    const char *metric_fragment = NULL;

    if (!PyArg_ParseTuple(args, "s:memcache_trace", &metric_fragment))
        return NULL;

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    if (self->transaction)
        rv = NRMemcacheTrace_New(self->transaction, metric_fragment);
    else
        rv = NRMemcacheTrace_New(NULL, NULL);

    if (rv == NULL)
        return NULL;

    return (PyObject *)rv;
}

static PyObject *NRTransaction_database_trace(
        NRTransactionObject *self, PyObject *args)
{
    NRDatabaseTraceObject *rv;

    const char *sql = NULL;

    if (!PyArg_ParseTuple(args, "s:database_trace", &sql))
        return NULL;

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    if (self->transaction)
        rv = NRDatabaseTrace_New(self->transaction, sql);
    else
        rv = NRDatabaseTrace_New(NULL, NULL);

    if (rv == NULL)
        return NULL;

    return (PyObject *)rv;
}
#endif

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_runtime_error(
        NRTransactionObject *self, PyObject *args)
{
    nr_transaction_error *record;

    PyObject *type = NULL;
    PyObject *value = NULL;
    PyObject *traceback = NULL;
    PyObject *params = NULL;

    PyObject *error_message = NULL;
    PyObject *stack_trace = NULL;

    /*
     * TODO Need to support keyword arguments so that can easily
     * pass parms after *sys.exc_info().
     */

    if (!PyArg_ParseTuple(args, "OOO!|O!:runtime_error", &type, &value,
                          &PyTraceBack_Type, &traceback, &PyDict_Type,
                          &params)) {
        return NULL;
    }

    if (self->transaction_state != NR_TRANSACTION_STATE_RUNNING) {
        PyErr_SetString(PyExc_RuntimeError, "transaction not active");
        return NULL;
    }

    if (type != Py_None && value != Py_None) {
        error_message = PyObject_Str(value);
        stack_trace = nrpy__format_exception(type, value, traceback);

        if (!stack_trace)
           PyErr_Clear();

        record = nr_transaction_error__allocate(
                self->transaction, &(self->transaction_errors), "", 0,
                PyString_AsString(error_message), Py_TYPE(value)->tp_name, 0);

        if (stack_trace) {
            nro__set_hash_string(record->params, "stack_trace",
                                 PyString_AsString(stack_trace));
        }

        if (params) {
            nrpy__merge_dict_into_params_at(record->params,
                                            "custom_parameters", params);
        }

        /*
	 * TODO There is also provision for passing back
	 * 'file_name', 'line_number' and 'source' params as
	 * well. These are dependent on RPM have been updated
         * to show them for something other than Ruby. The
         * passing back of such additional information as the
         * source code should be done by setting a flag and
         * not be on by default. The file name and line number
         * may not display in RPM the source code isn't also
         * sent. Need to see how RPM is changed. See details in:
         * https://www.pivotaltracker.com/story/show/7922639
         */

        /*
         * TODO Are there any default things that could be added
         * to the custom parameters for this unhandled exception
         * case. What about stack variables and values associated
         * with them. These should only be passed back though
         * if enabled through a flag.
         */

        Py_XDECREF(stack_trace);
        Py_DECREF(error_message);
    }

    Py_INCREF(Py_None);
    return Py_None;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_application(NRTransactionObject *self,
                                               void *closure)
{
    Py_INCREF(self->application);
    return (PyObject *)self->application;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_ignore(NRTransactionObject *self,
                                          void *closure)
{
    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return that transaction is
     * being ignored.
     */

    if (!self->transaction) {
        Py_INCREF(Py_True);
        return Py_True;
    }

    return PyBool_FromLong(self->transaction->ignore);
}

/* ------------------------------------------------------------------------- */

static int NRTransaction_set_ignore(NRTransactionObject *self,
                                    PyObject *value)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "can't delete ignore attribute");
        return -1;
    }

    if (!PyBool_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "expected bool for ignore attribute");
        return -1;
    }

    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return without actually doing
     * anything.
     */

    if (!self->transaction)
        return 0;

    if (value == Py_True)
        self->transaction->ignore = 1;
    else
        self->transaction->ignore = 0;

    return 0;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_path(NRTransactionObject *self,
                                        void *closure)
{
    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return None.
     */

    if (!self->transaction) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    return PyString_FromString(self->transaction->path);
}

/* ------------------------------------------------------------------------- */

static int NRTransaction_set_path(NRTransactionObject *self,
                                  PyObject *value)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError, "can't delete URL path attribute");
        return -1;
    }

    if (!PyString_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "expected string for "
                        "URL path attribute");
        return -1;
    }

    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return without actually doing
     * anything.
     */

    if (!self->transaction)
        return 0;

    /*
     * TODO We set path type to be 'CUSTOM' for now, but PHP
     * sets it different based on what it is being named with.
     * If a callable object it uses 'FUNCTION' and if a file
     * path then uses 'ACTION'. Do not understand the
     * differences and how that may be used in RPM GUI. The PHP
     * code also disallows the overriding of the path if already
     * set, the fact of it being set being recorded by
     * 'has_been_named' attribute of the transaction object. See:
     * https://www.pivotaltracker.com/story/show/9011677.
     */

    nrfree(self->transaction->path);

    self->transaction->path = nrstrdup(PyString_AsString(value));
    self->transaction->path_type = NR_PATH_TYPE_CUSTOM;

    return 0;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_has_been_named(NRTransactionObject *self,
                                                  void *closure)
{
    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return that transaction is
     * being ignored.
     */

    if (!self->transaction) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    return PyBool_FromLong(self->transaction->has_been_named);
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_custom_parameters(
        NRTransactionObject *self, void *closure)
{
    Py_INCREF(self->custom_parameters);

    return self->custom_parameters;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_request_parameters(
        NRTransactionObject *self, void *closure)
{
    Py_INCREF(self->request_parameters);

    return self->request_parameters;
}

/* ------------------------------------------------------------------------- */

static PyObject *NRTransaction_get_response_code(
        NRTransactionObject *self, void *closure)
{
    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return 0 as response code.
     */

    if (!self->transaction)
        return PyInt_FromLong(0);

    return PyInt_FromLong(self->transaction->http_response_code);
}

/* ------------------------------------------------------------------------- */

static int NRTransaction_set_response_code(
        NRTransactionObject *self, PyObject *value)
{
    if (value == NULL) {
        PyErr_SetString(PyExc_TypeError,
                        "can't delete response code attribute");
        return -1;
    }

    if (!PyInt_Check(value)) {
        PyErr_SetString(PyExc_TypeError, "expected integer for response code");
        return -1;
    }

    /*
     * If the application was not enabled and so we are running
     * as a dummy transaction then return without actually doing
     * anything.
     */

    if (!self->transaction)
        return 0;

    self->transaction->http_response_code = PyInt_AsLong(value);

    return 0;
}

/* ------------------------------------------------------------------------- */

#ifndef PyVarObject_HEAD_INIT
#define PyVarObject_HEAD_INIT(type, size) PyObject_HEAD_INIT(type) size,
#endif

static PyMethodDef NRTransaction_methods[] = {
    { "__enter__",          (PyCFunction)NRTransaction_enter,
                            METH_NOARGS, 0 },
    { "__exit__",           (PyCFunction)NRTransaction_exit,
                            METH_VARARGS, 0 },
#if 0
    { "function_trace",     (PyCFunction)NRTransaction_function_trace,
                            METH_VARARGS, 0 },
    { "external_trace",     (PyCFunction)NRTransaction_external_trace,
                            METH_VARARGS, 0 },
    { "memcache_trace",     (PyCFunction)NRTransaction_memcache_trace,
                            METH_VARARGS, 0 },
    { "database_trace",     (PyCFunction)NRTransaction_database_trace,
                            METH_VARARGS, 0 },
#endif
    { "runtime_error",      (PyCFunction)NRTransaction_runtime_error,
                            METH_VARARGS, 0 },
    { NULL, NULL }
};

static PyGetSetDef NRTransaction_getset[] = {
    { "application",        (getter)NRTransaction_get_application,
                            NULL, 0 },
    { "ignore",             (getter)NRTransaction_get_ignore,
                            (setter)NRTransaction_set_ignore, 0 },
    { "path",               (getter)NRTransaction_get_path,
                            (setter)NRTransaction_set_path, 0 },
    { "has_been_named",     (getter)NRTransaction_get_has_been_named,
                            NULL, 0 },
    { "custom_parameters",  (getter)NRTransaction_get_custom_parameters,
                            NULL, 0 },
    { "request_parameters", (getter)NRTransaction_get_request_parameters,
                            NULL, 0 },
    { "response_code",      (getter)NRTransaction_get_response_code,
                            (setter)NRTransaction_set_response_code, 0 },
    { NULL },
};

PyTypeObject NRTransaction_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "_newrelic.Transaction", /*tp_name*/
    sizeof(NRTransactionObject), /*tp_basicsize*/
    0,                      /*tp_itemsize*/
    /* methods */
    (destructor)NRTransaction_dealloc, /*tp_dealloc*/
    0,                      /*tp_print*/
    0,                      /*tp_getattr*/
    0,                      /*tp_setattr*/
    0,                      /*tp_compare*/
    0,                      /*tp_repr*/
    0,                      /*tp_as_number*/
    0,                      /*tp_as_sequence*/
    0,                      /*tp_as_mapping*/
    0,                      /*tp_hash*/
    0,                      /*tp_call*/
    0,                      /*tp_str*/
    0,                      /*tp_getattro*/
    0,                      /*tp_setattro*/
    0,                      /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT |
    Py_TPFLAGS_BASETYPE,    /*tp_flags*/
    0,                      /*tp_doc*/
    0,                      /*tp_traverse*/
    0,                      /*tp_clear*/
    0,                      /*tp_richcompare*/
    0,                      /*tp_weaklistoffset*/
    0,                      /*tp_iter*/
    0,                      /*tp_iternext*/
    NRTransaction_methods,  /*tp_methods*/
    0,                      /*tp_members*/
    NRTransaction_getset,   /*tp_getset*/
    0,                      /*tp_base*/
    0,                      /*tp_dict*/
    0,                      /*tp_descr_get*/
    0,                      /*tp_descr_set*/
    0,                      /*tp_dictoffset*/
    (initproc)NRTransaction_init, /*tp_init*/
    0,                      /*tp_alloc*/
    NRTransaction_new,      /*tp_new*/
    0,                      /*tp_free*/
    0,                      /*tp_is_gc*/
};

/* ------------------------------------------------------------------------- */
