# -*- coding: utf-8 -*-

"""Unit test package for shark."""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURES_DIR = os.path.abspath(os.path.join(BASE_DIR, 'fixtures'))
RTM_DATAPATH = os.path.abspath(os.path.join(FIXTURES_DIR, 'rtm_15m_w_gaps.json'))
