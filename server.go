package gorpc

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/lubanproj/gorpc/interceptor"
	"github.com/lubanproj/gorpc/log"
	"github.com/lubanproj/gorpc/plugin"
	"github.com/lubanproj/gorpc/plugin/jaeger"
)

// gorpc Server, a Server can have one or more Services
// gorpc 服务端
type Server struct {
	opts    *ServerOptions
	service Service
	plugins []plugin.Plugin

	closing bool // whether the server is closing
}

// NewServer creates a Server, Support to pass in ServerOption parameters
/**
 * 创建服务端
 *
 * NewServer
 * + RegisterService(serviceName string, svr interface{}) error	// 注册服务
 * + RegisterRegister(sd *ServiceDesc, svr interface{})			// 注册服务
 * + Serve()													// 启动 tcp 服务端
 * + ServeHttp()												// 启动 http 服务端
 * + Close()													// 关闭服务端
 * + InitPlugins() error 										//插件初始化
 *
 * 服务端流程:
 *
 * ServerOption(服务配置)						ServiceDesc(服务描述)
 *     |											 |
 * NewServer(创建服务端) -> NewService(创建服务) -> RegisterService(注册业务服务) -> Register(服务注册) -> InitPlugins(初始化插件) -> Serve / ServeHttp(启动服务)
 *
 * Server 是服务端，服务端的相关服务由 Service 实现。
 *
 */
func NewServer(opt ...ServerOption) *Server {
	/**
	 * 使用选项模式：
	 * 当使用默认配置的时候可以使用选项模式。
	 * 选项模式是使用一个操作配置的指针函数，通过with系列函数改变默认的配置的模式
	 */
	s := &Server{
		opts: &ServerOptions{},
	}

	/**
	 * o() 为 gorpc.ServerOption:
	 *
	 * type ServerOption func(*ServerOptions)
	 *
	 * 迭代 opt 修改默认的 option 配置
	 */
	for _, o := range opt {
		o(s.opts)
	}

	// 创建服务
	s.service = NewService(s.opts)

	/**
	 * 插件注册
	 *
	 * 通过 import 注册相关的插件到 plugin.PluginMap 中
	 * 如果 plugin.PluginMap 存在插件，把插件注册到Server中
	 *
	 */
	for pluginName, plugin := range plugin.PluginMap {
		if !containPlugin(pluginName, s.opts.pluginNames) {
			continue
		}
		s.plugins = append(s.plugins, plugin)
	}

	return s
}

func NewService(opts *ServerOptions) Service {
	return &service{
		opts: opts,
	}
}

func containPlugin(pluginName string, plugins []string) bool {
	for _, plugin := range plugins {
		if pluginName == plugin {
			return true
		}
	}
	return false
}

type emptyInterface interface{}

// 注册业务服务
func (s *Server) RegisterService(serviceName string, svr interface{}) error {

	svrType := reflect.TypeOf(svr)
	svrValue := reflect.ValueOf(svr)

	sd := &ServiceDesc{
		ServiceName: serviceName,
		// for compatibility with code generation
		HandlerType: (*emptyInterface)(nil),
		Svr:         svr,
	}

	methods, err := getServiceMethods(svrType, svrValue)
	if err != nil {
		return err
	}

	sd.Methods = methods

	s.Register(sd, svr)

	return nil
}

// 通过反射回去服务的方法
func getServiceMethods(serviceType reflect.Type, serviceValue reflect.Value) ([]*MethodDesc, error) {

	var methods []*MethodDesc

	for i := 0; i < serviceType.NumMethod(); i++ {
		method := serviceType.Method(i)

		if err := checkMethod(method.Type); err != nil {
			return nil, err
		}

		methodHandler := func(ctx context.Context, svr interface{}, dec func(interface{}) error, ceps []interceptor.ServerInterceptor) (interface{}, error) {

			reqType := method.Type.In(2)

			// determine type
			req := reflect.New(reqType.Elem()).Interface()

			if err := dec(req); err != nil {
				return nil, err
			}

			if len(ceps) == 0 {
				values := method.Func.Call([]reflect.Value{serviceValue, reflect.ValueOf(ctx), reflect.ValueOf(req)})
				// determine error
				return values[0].Interface(), nil
			}

			handler := func(ctx context.Context, reqbody interface{}) (interface{}, error) {

				values := method.Func.Call([]reflect.Value{serviceValue, reflect.ValueOf(ctx), reflect.ValueOf(req)})

				return values[0].Interface(), nil
			}

			return interceptor.ServerIntercept(ctx, req, ceps, handler)
		}

		methods = append(methods, &MethodDesc{
			MethodName: method.Name,
			Handler:    methodHandler,
		})
	}

	return methods, nil
}

// 通过反射检查服务的方法是否为方法类型
func checkMethod(method reflect.Type) error {

	// params num must >= 2 , needs to be combined with itself
	if method.NumIn() < 3 {
		return fmt.Errorf("method %s invalid, the number of params < 2", method.Name())
	}

	// return values nums must be 2
	if method.NumOut() != 2 {
		return fmt.Errorf("method %s invalid, the number of return values != 2", method.Name())
	}

	// the first parameter must be context
	ctxType := method.In(1)
	var contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	if !ctxType.Implements(contextType) {
		return fmt.Errorf("method %s invalid, first param is not context", method.Name())
	}

	// the second parameter type must be pointer
	argType := method.In(2)
	if argType.Kind() != reflect.Ptr {
		return fmt.Errorf("method %s invalid, req type is not a pointer", method.Name())
	}

	// the first return type must be a pointer
	replyType := method.Out(0)
	if replyType.Kind() != reflect.Ptr {
		return fmt.Errorf("method %s invalid, reply type is not a pointer", method.Name())
	}

	// The second return value must be an error
	errType := method.Out(1)
	var errorType = reflect.TypeOf((*error)(nil)).Elem()
	if !errType.Implements(errorType) {
		return fmt.Errorf("method %s invalid, returns %s , not error", method.Name(), errType.Name())
	}

	return nil
}

// 注册服务描述到服务中
func (s *Server) Register(sd *ServiceDesc, svr interface{}) {
	if sd == nil || svr == nil {
		return
	}
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(svr)
	if !st.Implements(ht) {
		log.Fatalf("handlerType %v not match service : %v ", ht, st)
	}

	ser := &service{
		svr:         svr,
		serviceName: sd.ServiceName,
		handlers:    make(map[string]Handler),
	}

	for _, method := range sd.Methods {
		ser.handlers[method.MethodName] = method.Handler
	}

	s.service = ser
}

func (s *Server) Serve() {

	err := s.InitPlugins()
	if err != nil {
		panic(err)
	}

	s.service.Serve(s.opts)

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGSEGV)
	<-ch

	s.Close()
}

type emptyService struct{}

func (s *Server) ServeHttp() {

	if err := s.RegisterService("/http", new(emptyService)); err != nil {
		panic(err)
	}

	s.Serve()
}

func (s *Server) Close() {
	s.closing = false

	s.service.Close()
}

func (s *Server) InitPlugins() error {
	// init plugins
	for _, p := range s.plugins {

		switch val := p.(type) {

		case plugin.ResolverPlugin:
			var services []string
			services = append(services, s.service.Name())

			pluginOpts := []plugin.Option{
				plugin.WithSelectorSvrAddr(s.opts.selectorSvrAddr),
				plugin.WithSvrAddr(s.opts.address),
				plugin.WithServices(services),
			}
			if err := val.Init(pluginOpts...); err != nil {
				log.Errorf("resolver init error, %v", err)
				return err
			}

		case plugin.TracingPlugin:

			pluginOpts := []plugin.Option{
				plugin.WithTracingSvrAddr(s.opts.tracingSvrAddr),
			}

			tracer, err := val.Init(pluginOpts...)
			if err != nil {
				log.Errorf("tracing init error, %v", err)
				return err
			}

			s.opts.interceptors = append(s.opts.interceptors, jaeger.OpenTracingServerInterceptor(tracer, s.opts.tracingSpanName))

		default:

		}

	}

	return nil
}
