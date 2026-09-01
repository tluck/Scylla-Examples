#!/usr/bin/env bash

year=${1:-2026}

cx list versions |grep ${year} |sort -n
