"""
Israeli-specific form helpers
"""
import re

from django.core.exceptions import ValidationError
from django.forms.fields import RegexField, Field, EMPTY_VALUES
from django.utils.translation import ugettext_lazy as _


id_number_re = re.compile(r'^(?P<number>\d{1,8})(?P<check>\d)$')

class ILPostalCodeField(RegexField):
    """
    A form field that validates its input as an Israeli postal code.
    Valid form is XXXXX where X represents integer.
    """

    default_error_messages = {
        'invalid': _(u'Enter a postal code in the format XXXXX'),
    }

    def __init__(self, *args, **kwargs):
        super(ILPostalCodeField, self).__init__(r'^\d{5}$', *args, **kwargs)

    def clean(self, value):
        return super(ILPostalCodeField, self).clean(value.replace(" ", ""))


class ILIDNumberField(Field):
    """
    A form field that validates its input as an Israeli identification number.
    Valid form is per the Israeli ID specification.
    """

    default_error_messages = {
        'invalid': _(u'Enter a valid ID number.'),
    }

    def clean(self, value):
        value = super(ILIDNumberField, self).clean(value)

        if value in EMPTY_VALUES:
            return u''

        match = id_number_re.match(value)
        if not match:
            raise ValidationError(self.error_messages['invalid'])

        number = match.groupdict()['number'].zfill(8)
        check = int(match.groupdict()['check'])

        sum = 0
        weight = 1
        for digit in number + str(check):
            sum += (lambda x: x/10 + x % 10)(int(digit)*weight)
            weight ^= 3

        if sum % 10 != 0:
            raise ValidationError(self.error_messages['invalid'])
        return value
