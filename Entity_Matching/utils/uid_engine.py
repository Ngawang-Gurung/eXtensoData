def char_to_digit(char):
    if char.isdigit():
        return int(char)
    elif char.isalpha():
        return (ord(char.lower()) - ord('a') + 1) % 10
    else:
        return 0

def string_to_digits(s):
    digits = [char_to_digit(char) for char in s]
    numeric_string = ''.join(map(str, digits))

    if len(numeric_string) > 13:
        return numeric_string[:13]
    else:
        return numeric_string.ljust(13, '0')