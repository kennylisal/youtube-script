from colorama import init, Fore, Back, Style

init(autoreset=True)

def line_print_error(text, end='\n\n'):
    print(Back.RED + Style.BRIGHT + text, end=end)

def line_print_success(text, end='\n\n'):
    print(Back.GREEN + Style.BRIGHT + text, end=end)

def line_print_warning(text, end='\n\n'):
    print(Back.YELLOW + Style.BRIGHT + text, end=end)

def line_print_announcement(text, end='\n\n'):
    print(Back.BLUE + Style.BRIGHT + text, end=end)