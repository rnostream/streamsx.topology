/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015,2016
*/

/*
 * Internal header file supporting Python
 * for com.ibm.streamsx.topology.
 *
 * This is not part of any public api for
 * the toolkit or toolkit with decorated
 * SPL Python operators.
 *
 * Functionality related to general
 * non-operator, non-data processing functions.
 */

#ifndef __SPL__SPLPY_GENERAL_H
#define __SPL__SPLPY_GENERAL_H

#include "Python.h"

#include <TopologySplpyResource.h>
#include <SPL/Runtime/Common/RuntimeException.h>
#include <SPL/Runtime/Type/Meta/BaseType.h>
#include <SPL/Runtime/ProcessingElement/PE.h>
#include <SPL/Runtime/Operator/Port/OperatorPort.h>
#include <SPL/Runtime/Operator/Port/OperatorInputPort.h>
#include <SPL/Runtime/Operator/Port/OperatorOutputPort.h>
#include <SPL/Runtime/Operator/OperatorContext.h>
#include <SPL/Runtime/Operator/Operator.h>

namespace streamsx {
  namespace topology {

    /*
    ** Convert to a SPL rstring from a Python string object.
    ** Returns 0 if successful, non-zero if error.
    */
    inline int pyRStringFromPyObject(SPL::rstring & attr, PyObject * value) {
      Py_ssize_t size = 0;
      PyObject * converted = NULL;
      char * bytes = NULL;

#if PY_MAJOR_VERSION == 3
      // Python 3 character strings are unicode objects
      // Caller is not responsible for deallocating buffer
      // returned by PyUnicode_AsUTF8AndSize
      //
      if (!PyUnicode_Check(value)) {
          // Create a string from the object
          value = converted = PyObject_Str(value);
      }
      bytes = PyUnicode_AsUTF8AndSize(value, &size);
#else
      // Python 2 supports Unicode and byte character strings 
      // Default is byte character strings.
      // PyString_AsStringAndSize returns a pointer to an
      // internal buffer that must not be modified or deallocated
      if (PyUnicode_Check(value)) {
          value = converted = PyUnicode_AsUTF8String(value);
      } else if (PyString_Check(value)) {
           // no coversion needed
      } else {
          // Create a string from the object
          value = converted = PyObject_Str(value);
      }
      int rc = PyString_AsStringAndSize(value, &bytes, &size);
      if (rc != 0)
          bytes = NULL;
#endif

      if (bytes == NULL) {
         if (converted != NULL)
             Py_DECREF(converted);
         return -1;
      }

      // This copies from bytes into the rstring.
      attr.assign((const char *)bytes, (size_t) size);

      // Need to decrement the reference after we
      // have copied the bytes out as bytes points
      // into the Python object.
      if (converted != NULL)
          Py_DECREF(converted);

      return 0;
    }

class PyGILLock {
   public:
        PyGILLock() {
          gstate_ = PyGILState_Ensure();
        }
        ~PyGILLock() {
          PyGILState_Release(gstate_);
        }
        
      private:
        PyGILState_STATE gstate_;
    };

class SplpyGeneral {
  public:
    /*
     * Flush Python stderr and stdout.
    */
    static void flush_PyErrPyOut() {
        PyRun_SimpleString("sys.stdout.flush()");
        PyRun_SimpleString("sys.stderr.flush()");
    }

    /*
    * Call PyErr_Print() and then flush stderr.
    * This is because CPython buffers stderr (and stdout)
    * when it is not connected to a terminal.
    * Without the flush output is lost in distributed
    * (since stderr is conncted to a file) and
    * makes diagnosing errors impossible.
    */
    static void flush_PyErr_Print() {
        PyErr_Print();
        SplpyGeneral::flush_PyErrPyOut();
    }

    /**
     * Return an SPL runtime exception that can be thrown
     * based upon the Python error. Also flushes Python stdout
     * and stderr to ensure any additional info is visible
     * in the PE console.
    */
    static SPL::SPLRuntimeException pythonException(std::string const & 	location) {
      PyObject *pyType, *pyValue, *pyTraceback;
      PyErr_Fetch(&pyType, &pyValue, &pyTraceback);

      if (pyType != NULL)
          Py_DECREF(pyType);
      if (pyTraceback != NULL)
          Py_DECREF(pyTraceback);

      SPL::rstring msg("Unknown Python error");
      if (pyValue != NULL) {
          streamsx::topology::pyRStringFromPyObject(msg, pyValue);
          Py_DECREF(pyValue);
      }

      SplpyGeneral::flush_PyErr_Print();

      SPL::SPLRuntimeOperatorException exc(location, msg);
      
      return exc;
    }

    /*
     * Load a function, returning the reference to the function.
     * Caller must hold the GILState
     */
    static PyObject * loadFunction(const char * moduleNameC, const char * functionNameC)
     {    
       PyObject * module = importModule(moduleNameC);
       PyObject * function = PyObject_GetAttrString(module, functionNameC);
       Py_DECREF(module);
    
       if (!PyCallable_Check(function)) {
         SPLAPPTRC(L_ERROR, "Fatal error: function " << functionNameC << " in module " << moduleNameC << " not callable", "python");
         throw;
        }
        SPLAPPTRC(L_INFO, "Callable function: " << functionNameC, "python");
        return function;
      }
   private:
    /*
     * Import a module, returning the reference to the module.
     * Caller must hold the GILState
     */
    static PyObject * importModule(const char * moduleNameC) 
    {
      PyObject * moduleName = PyUnicode_FromString(moduleNameC);
      PyObject * module = PyImport_Import(moduleName);
      Py_DECREF(moduleName);
      if (module == NULL) {
        SPLAPPLOG(L_ERROR, TOPOLOGY_IMPORT_MODULE_ERROR(moduleNameC), "python");
        throw SplpyGeneral::pythonException(moduleNameC);
      }
      SPLAPPLOG(L_INFO, TOPOLOGY_IMPORT_MODULE(moduleNameC), "python");
      return module;
    }
};

}}

#endif
