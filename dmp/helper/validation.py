#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'phongphamhong'

import os

import marshmallow.validate as marsh_validation
import wtforms.validators as wft_validation
from marshmallow import ValidationError, fields, Schema, validate
from marshmallow.validate import ValidationError

__all__ = [
    'NotEmpty',
    'ValidationError',
    'AllowExtension',
    'fields',
    'Schema',
    'wft_validation',
    'marsh_validation'
]


class NotEmpty(validate.Validator):
    default_message = "Input field must not be empty"

    def __init__(self, error=None):
        self.error = error or self.default_message

    def _format_error(self, value):
        return self.error.format(input=value)

    def __call__(self, value):
        message = self._format_error(value)
        if value is None or (type(value) is str and value.strip() == "") or (type(value) is not bool and not value):
            raise ValidationError(message=message)
        return True


class AllowExtension(validate.Validator):

    def __init__(self, allow_type: list = [], error=None):
        self.default_message = "Input file is invalid. Accepted: %s" % allow_type
        self.error = error or self.default_message
        self.allow_type = allow_type

    def _format_error(self, value):
        return self.error.format(input=value)

    def __call__(self, value):
        message = self._format_error(value)
        extension = os.path.splitext(value)
        extension = extension[1] if len(extension) >= 1 else ''
        extension = extension.lstrip('.')
        if self.allow_type and extension not in self.allow_type:
            raise ValidationError(message=message)
        return True


class MustIn(validate.Validator):
    def __init__(self, list_data: list = [], error=None):
        self.default_message = "Your data is invalid"
        self.error = error or self.default_message
        self.list_data = list_data

    def __call__(self, value):
        if value not in self.list_data:
            message = self._format_error(value)
            raise ValidationError(message=message)
        return True


class WTFMustIn(MustIn):
    def __call__(self, form, value):
        return super(WTFMustIn, self).__call__(value)


class WTFLength(wft_validation.Length):
    """
        Validates the length of a string.

        :param min:
            The minimum required length of the string. If not provided, minimum
            length will not be checked.
        :param max:
            The maximum length of the string. If not provided, maximum length
            will not be checked.
        :param message:
            Error message to raise in case of a validation error. Can be
            interpolated using `%(min)d` and `%(max)d` if desired. Useful defaults
            are provided depending on the existence of min and max.
    """

    def __call__(self, form, field):
        try:
            len(field.data)
        except BaseException as e:
            field.data = str(field.data)
        return super(WTFLength, self).__call__(form=form, field=field)


marsh_validation.NotEmpty = NotEmpty
marsh_validation.AllowExtension = AllowExtension
marsh_validation.MustIn = MustIn
wft_validation.Length = WTFLength
wft_validation.WTFMustIn = WTFMustIn
