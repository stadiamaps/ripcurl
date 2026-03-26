#!/usr/bin/env nu

# ripcurl smoke tests
#
# Runs the ripcurl binary against httpbin.org to verify basic functionality.
#
# Usage:
#   just smoke-test
#
# Environment variables:
#   RIPCURL_BIN  — path to ripcurl binary (default: ./target/debug/ripcurl)
#   TEST_SERVER  — base URL for HTTP test endpoints (default: https://httpbin.org but you can also run this in Docker)

use std assert

def main [] {
    let ripcurl = ($env | get -o RIPCURL_BIN | default "./target/debug/ripcurl")
    let server = ($env | get -o TEST_SERVER | default "https://httpbin.org")
    let tmp_dir = (mktemp -d)

    print $"ripcurl smoke tests"
    print $"  binary:  ($ripcurl)"
    print $"  server:  ($server)"
    print $"  tmp_dir: ($tmp_dir)"
    print ""

    let results = [
        (test_basic_download $ripcurl $server $tmp_dir)
        (test_large_download $ripcurl $server $tmp_dir)
        (test_range_capable_download $ripcurl $server $tmp_dir)
        (test_redirect_follow $ripcurl $server $tmp_dir)
        (test_404_error $ripcurl $server $tmp_dir)
        (test_invalid_url $ripcurl $tmp_dir)
    ]

    # Cleanup
    rm -rf $tmp_dir

    let passed = ($results | where success == true | length)
    let failed = ($results | where success == false | length)

    print ""
    print $"Results: ($passed) passed, ($failed) failed"

    if $failed > 0 {
        print ""
        print "Failures:"
        $results | where success == false | each { |r|
            print $"  - ($r.name): ($r.message)"
        }
        exit 1
    }
}

# Test: basic download of 1024 bytes
def test_basic_download [ripcurl: string, server: string, tmp_dir: string]: nothing -> record {
    let name = "basic_download"
    let dest = $"($tmp_dir)/basic.bin"
    let url = $"($server)/bytes/1024"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl $url $dest } | complete)

    if $result.exit_code != 0 {
        print "FAIL"
        return { name: $name, success: false, message: $"exit code ($result.exit_code): ($result.stderr)" }
    }

    let size = (ls $dest | get size.0)
    if ($size | into int) != 1024 {
        print "FAIL"
        return { name: $name, success: false, message: $"expected 1024 bytes, got ($size)" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}

# Test: larger download of 100KB
def test_large_download [ripcurl: string, server: string, tmp_dir: string]: nothing -> record {
    let name = "large_download"
    let dest = $"($tmp_dir)/large.bin"
    let expected = 102400
    let url = $"($server)/bytes/($expected)"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl $url $dest } | complete)

    if $result.exit_code != 0 {
        print "FAIL"
        return { name: $name, success: false, message: $"exit code ($result.exit_code): ($result.stderr)" }
    }

    let size = (ls $dest | get size.0 | into int)
    if $size != $expected {
        print "FAIL"
        return { name: $name, success: false, message: $"expected ($expected) bytes, got ($size)" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}

# Test: download from endpoint that supports Range requests
def test_range_capable_download [ripcurl: string, server: string, tmp_dir: string]: nothing -> record {
    let name = "range_capable_download"
    let dest = $"($tmp_dir)/range.bin"
    let url = $"($server)/range/1024"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl $url $dest } | complete)

    if $result.exit_code != 0 {
        print "FAIL"
        return { name: $name, success: false, message: $"exit code ($result.exit_code): ($result.stderr)" }
    }

    let size = (ls $dest | get size.0 | into int)
    if $size != 1024 {
        print "FAIL"
        return { name: $name, success: false, message: $"expected 1024 bytes, got ($size)" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}

# Test: follow redirects to final destination
def test_redirect_follow [ripcurl: string, server: string, tmp_dir: string]: nothing -> record {
    let name = "redirect_follow"
    let dest = $"($tmp_dir)/redirect.bin"
    let target = ($"($server)/bytes/512" | url encode)
    let url = $"($server)/redirect-to?url=($target)"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl $url $dest } | complete)

    if $result.exit_code != 0 {
        print "FAIL"
        return { name: $name, success: false, message: $"exit code ($result.exit_code): ($result.stderr)" }
    }

    let size = (ls $dest | get size.0 | into int)
    if $size != 512 {
        print "FAIL"
        return { name: $name, success: false, message: $"expected 512 bytes, got ($size)" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}

# Test: 404 should result in nonzero exit code
def test_404_error [ripcurl: string, server: string, tmp_dir: string]: nothing -> record {
    let name = "404_error"
    let dest = $"($tmp_dir)/notfound.bin"
    let url = $"($server)/status/404"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl $url $dest } | complete)

    if $result.exit_code == 0 {
        print "FAIL"
        return { name: $name, success: false, message: "expected nonzero exit code for 404" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}

# Test: invalid URL should have a nonzero exit code
def test_invalid_url [ripcurl: string, tmp_dir: string]: nothing -> record {
    let name = "invalid_url"
    let dest = $"($tmp_dir)/invalid.bin"

    print $"  ($name)... "

    let result = (do -i { ^$ripcurl "not-a-valid-url-scheme://????" $dest } | complete)

    if $result.exit_code == 0 {
        print "FAIL"
        return { name: $name, success: false, message: "expected nonzero exit code for invalid URL" }
    }

    print "ok"
    { name: $name, success: true, message: "ok" }
}
