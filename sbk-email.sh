#!/bin/bash
sbk --send-report --report-email=user@email.com --report-date=$(/bin/date +'\%Y-\%m-\%d')
