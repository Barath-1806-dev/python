def is_palindrome(s):
    # Remove spaces and convert to lowercase for a clean check
    s = s.replace(" ", "").lower()
    return s == s[::-1]

# Example usage
word = "Barath"
if is_palindrome(word):
    print(f"'{word}' is a palindrome!")
else:
    print(f"'{word}' is not a palindrome.")
