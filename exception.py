#
# Copyright (c) 2019 Wind River Systems, Inc.
#
# SPDX-License-Identifier: Apache-2.0
#


class RookClientException(Exception):
    message = "generic rook client exception"

    def __init__(self, *args, **kwargs):
        if "message" not in kwargs:
            try:
                message = self.message.format(*args, **kwargs)
            except Exception:   # noqa
                message = '{}, args:{}, kwargs: {}'.format(
                    self.message, args, kwargs)
        else:
            message = kwargs["message"]
        super(RookClientException, self).__init__(message)


class RookMonRestfulListKeysError(RookClientException):
    message = "Failed to get ceph-mgr restful plugin keys. {}"


class RookMonRestfulJsonError(RookClientException):
    message = "Failed to decode ceph-mgr restful plugin JSON response: {}"


class RookMonRestfulMissingUserCredentials(RookClientException):
    message = "Failed to get ceph-mgr restful plugin credentials for user: {}"


class RookMgrDumpError(RookClientException):
    message = "Failed to get ceph manager info. {}"


class RookMgrJsonError(RookClientException):
    message = "Failed to decode ceph manager JSON response: {}"


class RookMgrMissingRestfulService(RookClientException):
    message = "Missing restful service. Available services: {}"


class RookClientFormatNotSupported(RookClientException):
    message = "Command '{prefix}' does not support request format '{format}'"


class RookClientResponseFormatNotImplemented(RookClientException):
    message = ("Can't decode response. Support for '{format}' format "
               "is not implemented. Response: {reason}")


class RookClientFunctionNotImplemented(RookClientException):
    message = "Function '{name}' is not implemented"


class RookClientInvalidChoice(RookClientException):
    message = ("Function '{function}' does not support option "
               "{option}='{value}'. Supported values are: {supported}")


class RookClientTypeError(RookClientException):
    message = ("Expecting option '{name}' of type {expected}. "
               "Got {actual} instead")


class RookClientValueOutOfBounds(RookClientException):
    message = ("Argument '{name}' should be within range: {min} .. {max} "
               ". Got value '{actual}' instead")


class RookClientInvalidPgid(RookClientException):
    message = ("Argument '{name}' is not a valid Rook PG id. Expected "
               "n.xxx where n is an int > 0, xxx is a hex number > 0. "
               "Got value '{actual}' instead")


class RookClientInvalidIPAddr(RookClientException):
    message = ("Argument '{name}' should be a valid IPv4 or IPv6 address. "
               "Got value '{actual}' instead")


class RookClientInvalidOsdIdValue(RookClientException):
    message = ("Invalid OSD ID value '{osdid}'. Should start with 'osd.'")


class RookClientInvalidOsdIdType(RookClientException):
    message = ("Invalid OSD ID type for '{osdid}'. "
               "Expected integer or 'osd.NNN'")


class RookClientNoSuchUser(RookClientException):
    message = ("No such user '{user}'.")


class RookClientIncorrectPassword(RookClientException):
    message = ("Incorrect password for user '{user}'.")
