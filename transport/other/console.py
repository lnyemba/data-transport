from . import callback


class Writer (callback.Writer):
    def __init__(self,**_args):
        super().__init__(callback=print)
        