// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package redis

import "github.com/alibaba/RedisShake/pkg/libs/errors"

type Server struct {
	t HandlerTable
}

func NewServer(o interface{}) (*Server, error) {
	t, err := NewHandlerTable(o)
	if err != nil {
		return nil, err
	}
	return &Server{t}, nil
}

func NewServerWithTable(t HandlerTable) (*Server, error) {
	if t == nil {
		return nil, errors.Errorf("handler table is nil")
	}
	return &Server{t}, nil
}

func MustServer(o interface{}) *Server {
	return &Server{MustHandlerTable(o)}
}

func (s *Server) Dispatch(arg0 interface{}, resp Resp) (Resp, error) {
	if cmd, args, err := ParseArgs(resp); err != nil {
		return nil, err
	} else if f := s.t[cmd]; f == nil {
		return nil, errors.Errorf("unknown command '%s'", cmd)
	} else {
		return f(arg0, args...)
	}
}
