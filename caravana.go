package caravana

type stage interface {
	Start()
	Stop()
	getIn() any
	addOut(any)
}

type Caravana struct {
	stages map[stage]struct{}
}

func (c *Caravana) Link(stages ...stage) {
	if len(stages) == 0 {
		return
	}

	if c.stages == nil {
		c.stages = make(map[stage]struct{})
	}

	// registra todos
	for _, s := range stages {
		c.stages[s] = struct{}{}
	}

	// conecta sequencialmente
	for i := 1; i < len(stages); i++ {
		prev := stages[i-1]
		curr := stages[i]

		prev.addOut(curr.getIn())
	}
}

func (c *Caravana) Start() {
	for th, _ := range c.stages {
		th.Start()
	}
}

func (c *Caravana) Stop() {
	for th, _ := range c.stages {
		th.Stop()
	}
}
