#!/usr/bin/env bash


bambrycd() {
    cd `bambry -i $1 info -s`
}
