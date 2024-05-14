class Reference:
    def __init__(self, resource):
        """
        Create a new reference to another resource.
        :param reference_url: the (unique) url assigned to the referred resource.
        :param type: the resource type associated to the referred resource, e.g., Patient, Organization, etc.
        """
        super().__init__()
        self.reference = resource.get_url()
        self.type = type(resource)

