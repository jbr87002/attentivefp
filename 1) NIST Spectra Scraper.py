#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Download all IR spectra available from NIST Chemistry Webbook."""
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool
import tqdm


NIST_URL = 'http://webbook.nist.gov/cgi/cbook.cgi'
EXACT_RE = re.compile('/cgi/cbook.cgi\?GetInChI=(.*?)$')
ID_RE = re.compile('/cgi/cbook.cgi\?ID=(.*?)&')
#NOTE: Change these
JDX_PATH = 'jdx'
MOL_PATH = 'mol'

# Rate limiter - NIST allows 5 requests in a 30 second window
request_timestamps = deque(maxlen=5)

def rate_limited_request(*args, **kwargs):
    """Wrapper for requests.get that respects NIST's rate limit of 5 requests per 30 seconds."""
    current_time = datetime.now()
    
    # If we've made 5 requests already, check the oldest one
    if len(request_timestamps) == 5:
        oldest_request = request_timestamps[0]
        time_since_oldest = (current_time - oldest_request).total_seconds()
        
        # If the oldest request was less than 30 seconds ago, wait
        if time_since_oldest < 30:
            sleep_time = 30 - time_since_oldest
            time.sleep(sleep_time)
    
    # Make the request and record the timestamp
    response = requests.get(*args, **kwargs)
    request_timestamps.append(datetime.now())
    
    return response

def search_nist_formula(formula, allow_other = False, allow_extra = False, match_isotopes = True, exclude_ions = False, has_ir = True):
    """Search NIST using the specified formula query and return the matching NIST IDs."""
    print('Searching: %s' % formula)
    params = {'Formula': formula, 'Units': 'SI'}
    if allow_other:
        params['AllowOther'] = 'on'
    if allow_extra:
        params['AllowExtra'] = 'on'
    if match_isotopes:
        params['MatchIso'] = 'on'
    if exclude_ions:
        params['NoIon'] = 'on'
    if has_ir:
        params['cIR'] = 'on'
    response = rate_limited_request(NIST_URL, params=params)
    soup = BeautifulSoup(response.text, 'html.parser')
    ids = [re.match(ID_RE, link['href']).group(1) for link in soup('a', href = ID_RE)]
    print('Result: %s' % ids)
    return ids


def get_jdx(nistid, stype = "IR"):
    """Download jdx file for the specified NIST ID, unless already downloaded."""
    filepath = os.path.join(JDX_PATH, '%s-%s.jdx' % (nistid, stype))
    if os.path.isfile(filepath):
        print('%s %s: Already exists at %s' % (nistid, stype, filepath))
        return
    print('%s %s: Downloading' % (nistid, stype))
    response = rate_limited_request(NIST_URL, params={'JCAMP': nistid, 'Type': stype, 'Index': 0})
    if response.text == '##TITLE=Spectrum not found.\n##END=\n':
        print('%s %s: Spectrum not found' % (nistid, stype))
        return
    print('Saving %s' % filepath)
    with open(filepath, 'wb') as file:
        file.write(response.content)


def get_mol(nistid):
    """Download mol file for the specified NIST ID, unless already downloaded."""
    filepath = os.path.join(MOL_PATH, '%s.mol' % nistid)
    if os.path.isfile(filepath):
        print('%s: Already exists at %s' % (nistid, filepath))
        return
    print('%s: Downloading mol' % nistid)
    response = rate_limited_request(NIST_URL, params={'Str2File': nistid})
    if response.text == 'NIST    12121112142D 1   1.00000     0.00000\nCopyright by the U.S. Sec. Commerce on behalf of U.S.A. All rights reserved.\n0  0  0     0  0              1 V2000\nM  END\n':
        print('%s: MOL not found' % nistid)
        return
    print('Saving %s' % filepath)
    with open(filepath, 'wb') as file:
        file.write(response.content)

def retreive_data_from_formula(formula):
    ids = search_nist_formula(formula, allow_other = True, exclude_ions = False, has_ir = True)
    for nistid in ids:
        get_mol(nistid)
        get_jdx(nistid)

def get_all_IR():
    """Search NIST for all structures with IR Spectra and download a JDX + Mol file for each."""
    # Create directories if they don't exist
    os.makedirs(JDX_PATH, exist_ok=True)
    os.makedirs(MOL_PATH, exist_ok=True)
    
    formulae = []
    IDs = []
    with open("species.txt") as data_file:
        entries = data_file.readlines()
        for entry in entries:
            try:
                formulae.append(entry.split()[-2])
            except:
                IDs.append(entry.strip())
    
    # Process formulas sequentially rather than in parallel to respect rate limits
    print(f"Processing {len(formulae)} formulas...")
    # load in a set of done formulae
    # if already done, skip
    # if not done, add to done list
    done_formulae = set()
    with open("done_formulae.txt", "r") as done_file:
        done_formulae = set(line.strip() for line in done_file)
    for formula in tqdm.tqdm(formulae, total=len(formulae)):
        if formula in done_formulae:
            continue
        retreive_data_from_formula(formula)
        with open("done_formulae.txt", "a") as done_file:
            done_file.write(formula + "\n")
    print("Done with formulas!")
    
    print(f"Processing {len(IDs)} IDs...")
    # load in a set of done IDs
    # if already done, skip
    # if not done, add to done list
    done_IDs = set()
    with open("done_IDs.txt", "r") as done_file:
        done_IDs = set(line.strip() for line in done_file)
    for nistid in tqdm.tqdm(IDs, total=len(IDs)):
        if nistid in done_IDs:
            continue
        get_mol(nistid)
        get_jdx(nistid)
        with open("done_IDs.txt", "a") as done_file:
            done_file.write(nistid + "\n")
    print("Done Scraping Data!")

if __name__ == '__main__':
    get_all_IR()
