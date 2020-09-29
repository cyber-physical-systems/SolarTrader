# Logger
class Logger(object):
    DEBUG = True
    groups = {}

    def __init__(self, msg):
        pass
    
    @classmethod
    def debug_on(cls):
        cls.DEBUG = True

    @classmethod
    def debug_off(cls):
        cls.DEBUG = False

    @classmethod
    def print(cls, *args):
        if not cls.DEBUG:
            return
        
        args_length = len(args)
        if args_length == 1:
            print(args[0])
        elif args_length == 2:
            if args[1] not in cls.groups:
                cls.set_group(args[1], True)
        
            if cls.DEBUG and cls.groups[args[1]]:
                print(args[0])
    
    @classmethod
    def set_group(cls, group, status):
        if group in cls.groups:
            print("LOGGER ERROR: group is exist!")
        else:
            cls.groups[group] = status
    
    @classmethod
    def list_group(cls):
        print(cls.groups)

    @classmethod
    def mute(cls, group):
        cls.groups[group] = False
        
    
    @classmethod
    def unmute(cls, group):
        cls.groups[group] = True

    @classmethod
    def show_process(cls, counter, total):
        percentage = round(counter/total*100)
        print(f'{counter}/{total} ({percentage}%)', end='\r')
        if counter >= total:
            print(f'{counter}/{total} ({percentage}%)')

