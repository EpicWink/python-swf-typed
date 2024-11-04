*swf-typed* - Python interface to SWF
=====================================

Typed Python interface to `AWS Simple Workflow Service <https://aws.amazon.com/swf/>`_.

* Type annotations
* Explicit exceptions
* Execution state construction
* Consistent method/attribute/parameter names (see below)
* Consistent model struture
* Automatic flattening of paged-list responses (next-page calls are run concurrently and
  on-demand)
* Better execution filtering

The most interesting part is state construction: given an execution history, this
library can build a full state of the execution and all of its tasks with all details;
see :meth:`swf_typed.build_state`). The rest of the API simply wraps and closely
matches AWS's SWF API.


See also
^^^^^^^^

* `py-swf <https://pypi.org/project/py-swf/>`_ - typed and object-oriented interface
  layer
* `mypy-boto3-swf <https://pypi.org/project/mypy-boto3-swf/>`_ - type-annotated layer
* `python-simple-workflow <https://pypi.org/project/simple-workflow/>`_ - higher-level
  interface layer


Installation
------------

.. code-block:: shell

   pip install swf-typed


Documentation
-------------

.. toctree::
   :maxdepth: 1

   API Reference <swf_typed>


Example
^^^^^^^

.. code-block:: python

   import swf_typed

   execution = swf_typed.ExecutionId(id="spam", run_id="abcd1234")
   execution_details = swf_typed.describe_execution(execution, domain="eggs")
   print(execution_details.configuration)

   events = swf_typed.get_execution_history(execution, domain="eggs")
   state = swf_typed.build_state(events)
   for task in state.tasks:
       print(task.status)

Terminology
^^^^^^^^^^^

This library has a slight change in terminology from AWS `SDKs
<https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/swf.html>`_/`APIs
<https://docs.aws.amazon.com/amazonswf/latest/apireference/Welcome.html>`_/`docs <https://docs.aws.amazon.com/amazonswf/latest/developerguide/swf-welcome.html>`_:

* Workflow type -> workflow
* Workflow execution -> execution
* Workflow execution ``workflowId`` -> execution ID
* Activity type -> activity
* Activity task -> task
* Activity worker -> worker
* Activity task ``activityId`` -> task ID

This is to simplify symbol names.
