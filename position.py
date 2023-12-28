class Position:
    updated_at: float = 0.0 # default, to identify other value is just init or reset to zero value
    def __init__(
        self,
        current_position: float = 0.0,
        target_position: float = 0.0,
        entry_price: float = 0.0,
        current_value: float = 0.0
    ):
        self.current_position = current_position
        self.target_position = target_position
        self.entry_price = entry_price
        self.current_value = current_value
    
    def set_current_position(self, new_value: float):
        self.current_position = new_value
        self.updated_at = time.time()
    
    def set_target_position(self, new_value: float):
        self.target_position = new_value
        self.updated_at = time.time()
    
    def set_current_value(self, new_value: float):
        self.current_value = new_value

    def to_json(self):
        return orjson.dumps(self, default=lambda o: o.__dict__, option=orjson.OPT_SERIALIZE_NUMPY)

    def __repr__(self) -> str:
        return f"Position: current: {self.current_position}, target: {self.target_position}, value: {self.current_value}, entry price: {self.entry_price}"
    



    