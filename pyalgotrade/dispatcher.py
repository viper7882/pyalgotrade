# PyAlgoTrade
#
# Copyright 2011-2015 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from pyalgotrade import utils
from pyalgotrade import observer
from pyalgotrade import dispatchprio


# This class is responsible for dispatching events from multiple subjects, synchronizing them if necessary.
class Dispatcher(object):
    def __init__(self):
        self.__subjects = []
        self.__stop = False
        self.__startEvent = observer.Event()
        self.__idleEvent = observer.Event()
        self.__currDateTime = None
        self.__started = False
        self.__ended = False

    # Returns the current event datetime. It may be None for events from realtime subjects.
    def getCurrentDateTime(self):
        return self.__currDateTime

    def getStartEvent(self):
        return self.__startEvent

    def getIdleEvent(self):
        return self.__idleEvent

    def stop(self):
        print ('step, stop')
        self.__stop = True

    def getSubjects(self):
        return self.__subjects

    def addSubject(self, subject):
        # Skip the subject if it was already added.
        if subject in self.__subjects:
            return

        # If the subject has no specific dispatch priority put it right at the end.
        if subject.getDispatchPriority() is dispatchprio.LAST:
            print ('addSubject, append type subject = ', str(type(subject)), ', value subject = ', str(subject))
            self.__subjects.append(subject)
        else:
            # Find the position according to the subject's priority.
            pos = 0
            for s in self.__subjects:
                if s.getDispatchPriority() is dispatchprio.LAST or subject.getDispatchPriority() < s.getDispatchPriority():
                    break
                pos += 1
            print ('addSubject, insert type subject = ', str(type(subject)), ', value subject = ', str(subject))
            self.__subjects.insert(pos, subject)

        subject.onDispatcherRegistered(self)

    # Return True if events were dispatched.
    def __dispatchSubject(self, subject, currEventDateTime):
        ret = False
        # Dispatch if the datetime is currEventDateTime of if its a realtime subject.
        if not subject.eof() and subject.peekDateTime() in (None, currEventDateTime):
            print ('__dispatchSubject -> dispatch, type subject = ', str(type(subject)), ', value subject = ', str(subject))
            ret = subject.dispatch() is True
        return ret

    # Returns a tuple with booleans
    # 1: True if all subjects hit eof
    # 2: True if at least one subject dispatched events.
    def __dispatch(self):
        smallestDateTime = None
        eof = True
        eventsDispatched = False

        # Scan for the lowest datetime.
        for subject in self.__subjects:
            if not subject.eof():
                eof = False
                smallestDateTime = utils.safe_min(smallestDateTime, subject.peekDateTime())

        # Dispatch realtime subjects and those subjects with the lowest datetime.
        if not eof:
            self.__currDateTime = smallestDateTime

            for subject in self.__subjects:
                print ('__dispatch -> __dispatchSubject, type subject = ', str(type(subject)), ', value subject = ', str(subject))
                if self.__dispatchSubject(subject, smallestDateTime):
                    eventsDispatched = True
        return eof, eventsDispatched

    def run(self):
        try:
            for subject in self.__subjects:
                subject.start()

            self.__startEvent.emit()

            while not self.__stop:
                eof, eventsDispatched = self.__dispatch()
                if eof:
                    self.__stop = True
                elif not eventsDispatched:
                    self.__idleEvent.emit()
        finally:
            for subject in self.__subjects:
                subject.stop()
            for subject in self.__subjects:
                subject.join()

    def step(self):
        """Can call multiple times to step the strategy."""
        """Return True if can be continued, else False"""
        if self.__ended == False:
            if self.__started == False:
                for subject in self.__subjects:
                    subject.start()

                self.__startEvent.emit()

            print ('step, self.__stop = ', str(self.__stop))
            if not self.__stop:
                eof, eventsDispatched = self.__dispatch()
                print ('step, type eof = ', str(type(eof)), ', value eof = ', str(eof), ', type eventsDispatched = ', str(type(eventsDispatched)), ', value eventsDispatched = ', str(eventsDispatched))
                if eof:
                    print ('step, eof = ', str(eof))
                    self.__stop = True
                elif not eventsDispatched:
                    self.__idleEvent.emit()

            if (self.__ended == False) and (self.__stop == True):
                for subject in self.__subjects:
                    subject.stop()
                for subject in self.__subjects:
                    subject.join()
                self.__ended = True
                print ('step, self.__ended = ', str(self.__ended))
        print ('dispatcher, step, __started = ', str(self.__started), ', __stop = ', str(self.__stop), ', __ended = ', str(self.__ended))
        return not self.ended()

    def ended(self):
        """Return True if can be continued, else False"""
        print ('dispatcher, ended, self.__ended = ', str(self.__ended))
        return self.__ended
