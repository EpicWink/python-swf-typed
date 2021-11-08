*swf-typed* - Python interface to SWF
=====================================

Typed Python interface to AWS Simple Workflow service

* Type annotations
* Explicit exceptions
* Execution state construction
* Consistent method/attribute/parameter names (see below)
* Consistent model struture
* Automatic flattening of paged-list responses (next-page calls are run concurrently and
  on-demand)
* Better execution filtering


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

This library uses a slight terminology change from SWF SDKs/APIs:

* Workflow type -> workflow
* Workflow execution -> execution
* Workflow execution ``workflowId`` -> execution ID
* Activity type -> activity
* Activity task -> task
* Activity worker -> worker
* Activity task ``activityId`` -> task ID
