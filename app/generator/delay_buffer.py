import random

class DelayBuffer:
    def __init__(self,delay_prob=0.2):
        self.buffer=[]
        self.delay_prob=delay_prob

    def process(self,event):
        #each event goes through buffer
        self.buffer.append(event)

        output=[]

        # random probability >0.2 let the event go
        if random.random()>self.delay_prob:
            output.append(self.buffer.pop(0))
        # after three random events generated with prob > 0.2 take one and let go
        if len(self.buffer)>3:
            idx=random.randint(0,len(self.buffer)-1)
            output.append(self.buffer.pop(idx))

        return output