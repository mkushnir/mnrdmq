Simple Controller/Agent Framework
=================================

A pair of controller and agent entities that commununicate via Redis
publish/subscriber facility.

Protocol
========

Base protocol defines the following events:

- discover: controller broadcasts the discover request in order to collect
  all knoen agents.  This is normally during controller's initial serve
  sequenve.  Agent handles the event by sending the join event.

- join: agent joins the realm.  Controller handles the event by
  registering the agent's capabilities and join time.

- leave: agent leaves the realm.  Controller handles the event by
  registring the agent's leave time.

- status:

- - agent reports its status.  Controller handles the event by updating
    the agent's status field and the update time.

- - controller requests status from agent(s).  An agent reports its
    status.  Controller handler the event as described above.


TODO
----

Protocol versioning.


Controller
==========

Base controller provides simple implementation of the protocol.


Agent
=====

Base agent provides simple implementation of the protocol.
