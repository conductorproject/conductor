.. conductor documentation master file, created by
   sphinx-quickstart on Sat Apr  4 14:32:02 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Conductor
=========

Conductor is a generic framework for processing lines that generate Earth
Observation products.

Conductor uses processing lines to manage tasks. A task may use several input
resources and it may produce several outputs. Each resource can be fetched from
a multitude of sources using the concept of movers.

A processing line is put together by crafting dedicated settings files.

Settings files
--------------

Conductor reads settings from local files or from remote API calls. The
settings are specified using JSON.

Conductor concepts
------------------

processing_lines
................

processing_tasks
................

A task represents a processing step in a processing line. It may have inputs
and outputs. Each input or output may be restricted by the current execution
date and time. A task is runnable in three distinct modes:

* creation mode - this is the main mode. It is used for generating the actual
  task outputs

* moving mode - this mode is used to send the outputs of a task to other
  locations

* deletion mode - this mode is used to clean up the local filesystem. It
  deletes the task's outputs that may exist

* combined mode - this mode is simply the sequential running of the creation,
  moving and deletion modes in a single go.

This object holds settings for the various tasks that compose a processing line.
Each task is a JSON object with the following structure:

.. code:: javascript

   "<task_name>": {
       "description": "<human readable description for the task>",
       "inputs": [
           {
               "name": "<file_resource name>",
               "strategy": {
                   <strategy_settings>
               }
               "except_when": {
                   "years": [<year>]
                   "months": [<month>]
                   "days": [<day>]
                   "hours": [<hour>]
                   "minutes": [<minute>]
                   "dekades": [<dekade>]
               }
               "optional_when": {
                   "years": [<year>]
                   "months": [<month>]
                   "days": [<day>]
                   "hours": [<hour>]
                   "minutes": [<minute>]
                   "dekades": [<dekade>]
               }
               "filtering_rules": [<rule>]
               "copy_to_working_dir": <boolean>
           }
       ],
       "outputs": [<similar structure as the inputs>],
       "run_modes": {
           "CREATION_MODE": {
               "execution_code": "<python path to the code>"
               "parameters": {}
           }
           "DELETION_MODE"
           "MOVING_MODE"
       }
   }

* task_name - Each task must have a unique name. This is used as an identifier
  for the task when configuring processing lines;
* description - A human-readable string specifying a brief description of the
  task;
* inputs - An object specifying details regarding the task's input resources.
* outputs - An object specifying details regarding the task's output resources.
* run_modes - An object with the configuration for the three run modes that
  a task supports.


file_resources
..............

movers
......


Contents:

.. toctree::
   :maxdepth: 2



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

