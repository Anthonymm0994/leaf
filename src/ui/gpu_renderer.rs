use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use egui::{Color32, Pos2, Vec2};
use glam::{Vec2 as GlamVec2, Vec3 as GlamVec3, Vec4 as GlamVec4};
use wgpu::{
    Adapter, Backends, Device, DeviceDescriptor, Features, Instance, Limits, Queue, RequestAdapterOptions,
    Surface, SurfaceConfiguration, TextureFormat, TextureUsages, TextureViewDescriptor,
};
use wgpu::util::DeviceExt;
use winit::window::Window;

// Vertex types for GPU rendering
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct LineVertex {
    pub position: [f32; 2],
    pub strip_index: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PointVertex {
    pub position: [f32; 2],
    pub color: [f32; 4],
    pub size: f32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct ShapeVertex {
    pub position: [f32; 2],
    pub color: [f32; 4],
}

// GPU rendering commands
#[derive(Debug, Clone)]
pub enum RenderCommand {
    Clear { color: [f32; 4] },
    DrawLines { 
        buffer_id: BufferId,
        count: u32,
        color: [f32; 4],
        width: f32,
        style: LineStyle,
    },
    DrawPoints {
        buffer_id: BufferId,
        count: u32,
        color: [f32; 4],
        size: f32,
        shape: PointShape,
    },
    DrawShapes {
        buffer_id: BufferId,
        count: u32,
        color: [f32; 4],
        filled: bool,
    },
}

// Rendering primitives
#[derive(Debug, Clone, Copy)]
pub enum LineStyle {
    Solid,
    Dashed,
    Dotted,
}

#[derive(Debug, Clone, Copy)]
pub enum PointShape {
    Circle,
    Square,
    Diamond,
    Triangle,
}

// Buffer management
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BufferId(pub u32);

// GPU renderer capabilities
#[derive(Debug, Clone)]
pub struct GpuCapabilities {
    pub max_vertices: usize,
    pub max_instances: usize,
    pub supports_instancing: bool,
    pub supports_compute: bool,
    pub max_texture_size: u32,
}

// Main GPU renderer
pub struct GpuRenderer<'a> {
    device: Device,
    queue: Queue,
    surface: Surface<'a>,
    config: SurfaceConfiguration,
    
    // Resource management
    vertex_buffers: HashMap<BufferId, wgpu::Buffer>,
    index_buffers: HashMap<BufferId, wgpu::Buffer>,
    uniform_buffers: HashMap<BufferId, wgpu::Buffer>,
    
    // Rendering state
    current_frame: Option<wgpu::SurfaceTexture>,
    depth_texture: wgpu::Texture,
    msaa_texture: wgpu::Texture,
    
    // Capabilities
    capabilities: GpuCapabilities,
}

impl<'a> GpuRenderer<'a> {
    pub async fn new(window: &'a Window) -> Result<Self> {
        let instance = Instance::new(wgpu::InstanceDescriptor {
            backends: Backends::all(),
            dx12_shader_compiler: Default::default(),
            flags: wgpu::InstanceFlags::default(),
            gles_minor_version: wgpu::Gles3MinorVersion::Automatic,
        });

        let surface = unsafe { instance.create_surface(window) }?;
        let adapter = instance
            .request_adapter(&RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::default(),
                force_fallback_adapter: false,
                compatible_surface: Some(&surface),
            })
            .await
            .ok_or_else(|| anyhow!("Failed to find an appropriate adapter"))?;

        let (device, queue) = adapter
            .request_device(
                &DeviceDescriptor {
                    label: None,
                    required_features: Features::empty(),
                    required_limits: Limits::default(),
                },
                None,
            )
            .await?;

        let surface_caps = surface.get_capabilities(&adapter);
        let surface_format = surface_caps
            .formats
            .iter()
            .copied()
            .find(|f| f.is_srgb())
            .unwrap_or(surface_caps.formats[0]);

        let config = SurfaceConfiguration {
            usage: TextureUsages::RENDER_ATTACHMENT,
            format: surface_format,
            width: window.inner_size().width,
            height: window.inner_size().height,
            present_mode: surface_caps.present_modes[0],
            alpha_mode: surface_caps.alpha_modes[0],
            view_formats: vec![],
            desired_maximum_frame_latency: 2,
        };
        surface.configure(&device, &config);

        // Create depth texture
        let depth_texture = device.create_texture(&wgpu::TextureDescriptor {
            size: wgpu::Extent3d {
                width: config.width,
                height: config.height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: wgpu::TextureFormat::Depth32Float,
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::TEXTURE_BINDING,
            label: Some("depth_texture"),
            view_formats: &[],
        });

        // Create MSAA texture
        let msaa_texture = device.create_texture(&wgpu::TextureDescriptor {
            size: wgpu::Extent3d {
                width: config.width,
                height: config.height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 4,
            dimension: wgpu::TextureDimension::D2,
            format: config.format,
            usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
            label: Some("msaa_texture"),
            view_formats: &[],
        });

        let capabilities = GpuCapabilities {
            max_vertices: adapter.limits().max_vertex_buffer_array_stride as usize / std::mem::size_of::<LineVertex>(),
            max_instances: adapter.limits().max_vertex_buffer_array_stride as usize / std::mem::size_of::<PointVertex>(),
            supports_instancing: true,
            supports_compute: false, // wgpu 0.20: no Features::COMPUTE_SHADERS
            max_texture_size: adapter.limits().max_texture_dimension_1d,
        };

        Ok(Self {
            device,
            queue,
            surface,
            config,
            vertex_buffers: HashMap::new(),
            index_buffers: HashMap::new(),
            uniform_buffers: HashMap::new(),
            current_frame: None,
            depth_texture,
            msaa_texture,
            capabilities,
        })
    }

    pub fn resize(&mut self, width: u32, height: u32) {
        if width > 0 && height > 0 {
            self.config.width = width;
            self.config.height = height;
            self.surface.configure(&self.device, &self.config);
            
            // Recreate depth and MSAA textures
            self.depth_texture = self.device.create_texture(&wgpu::TextureDescriptor {
                size: wgpu::Extent3d {
                    width: self.config.width,
                    height: self.config.height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: wgpu::TextureFormat::Depth32Float,
                usage: wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::TEXTURE_BINDING,
                label: Some("depth_texture"),
                view_formats: &[],
            });

            self.msaa_texture = self.device.create_texture(&wgpu::TextureDescriptor {
                size: wgpu::Extent3d {
                    width: self.config.width,
                    height: self.config.height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 4,
                dimension: wgpu::TextureDimension::D2,
                format: self.config.format,
                usage: wgpu::TextureUsages::RENDER_ATTACHMENT,
                label: Some("msaa_texture"),
                view_formats: &[],
            });
        }
    }

    pub fn render(&mut self, commands: &[RenderCommand]) -> Result<()> {
        let frame = self.surface.get_current_texture()?;
        let view = frame.texture.create_view(&TextureViewDescriptor::default());
        let depth_view = self.depth_texture.create_view(&TextureViewDescriptor::default());
        let msaa_view = self.msaa_texture.create_view(&TextureViewDescriptor::default());
        let mut encoder = self.device.create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("render_encoder"),
        });
        // Create pipelines before the render pass so they live long enough
        let line_pipeline = self.create_line_pipeline();
        let point_pipeline = self.create_point_pipeline();
        let shape_pipeline = self.create_shape_pipeline();
        {
            let mut render_pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("render_pass"),
                color_attachments: &[Some(wgpu::RenderPassColorAttachment {
                    view: &msaa_view,
                    resolve_target: Some(&view),
                    ops: wgpu::Operations {
                        load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                        store: wgpu::StoreOp::Store,
                    },
                })],
                depth_stencil_attachment: Some(wgpu::RenderPassDepthStencilAttachment {
                    view: &depth_view,
                    depth_ops: Some(wgpu::Operations {
                        load: wgpu::LoadOp::Clear(1.0),
                        store: wgpu::StoreOp::Store,
                    }),
                    stencil_ops: None,
                }),
                occlusion_query_set: None,
                timestamp_writes: None,
            });
            for command in commands {
                match command {
                    RenderCommand::Clear { .. } => {
                        render_pass.set_pipeline(&line_pipeline);
                        // Clear with color (not implemented)
                    }
                    RenderCommand::DrawLines { buffer_id, count, .. } => {
                        if let Some(buffer) = self.vertex_buffers.get(buffer_id) {
                            render_pass.set_pipeline(&line_pipeline);
                            render_pass.set_vertex_buffer(0, buffer.slice(..));
                            render_pass.draw(0..*count, 0..1);
                        }
                    }
                    RenderCommand::DrawPoints { buffer_id, count, .. } => {
                        if let Some(buffer) = self.vertex_buffers.get(buffer_id) {
                            render_pass.set_pipeline(&point_pipeline);
                            render_pass.set_vertex_buffer(0, buffer.slice(..));
                            render_pass.draw(0..*count, 0..1);
                        }
                    }
                    RenderCommand::DrawShapes { buffer_id, count, .. } => {
                        if let Some(buffer) = self.vertex_buffers.get(buffer_id) {
                            render_pass.set_pipeline(&shape_pipeline);
                            render_pass.set_vertex_buffer(0, buffer.slice(..));
                            render_pass.draw(0..*count, 0..1);
                        }
                    }
                }
            }
        }
        self.queue.submit(std::iter::once(encoder.finish()));
        frame.present();
        Ok(())
    }

    pub fn create_line_batch(&mut self, vertices: &[LineVertex]) -> BufferId {
        let buffer_id = BufferId(self.vertex_buffers.len() as u32);
        let buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("line_vertex_buffer"),
            contents: bytemuck::cast_slice(vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        self.vertex_buffers.insert(buffer_id, buffer);
        buffer_id
    }

    pub fn create_point_batch(&mut self, vertices: &[PointVertex]) -> BufferId {
        let buffer_id = BufferId(self.vertex_buffers.len() as u32);
        let buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("point_vertex_buffer"),
            contents: bytemuck::cast_slice(vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        self.vertex_buffers.insert(buffer_id, buffer);
        buffer_id
    }

    pub fn create_shape_batch(&mut self, vertices: &[ShapeVertex]) -> BufferId {
        let buffer_id = BufferId(self.vertex_buffers.len() as u32);
        let buffer = self.device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some("shape_vertex_buffer"),
            contents: bytemuck::cast_slice(vertices),
            usage: wgpu::BufferUsages::VERTEX,
        });
        self.vertex_buffers.insert(buffer_id, buffer);
        buffer_id
    }

    pub fn capabilities(&self) -> &GpuCapabilities {
        &self.capabilities
    }

    // Pipeline creation methods
    fn create_line_pipeline(&self) -> wgpu::RenderPipeline {
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("line_shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/line.wgsl").into()),
        });

        self.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("line_pipeline"),
            layout: None,
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: "vs_main",
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<LineVertex>() as wgpu::BufferAddress,
                    step_mode: wgpu::VertexStepMode::Vertex,
                    attributes: &[
                        wgpu::VertexAttribute {
                            offset: 0,
                            shader_location: 0,
                            format: wgpu::VertexFormat::Float32x2,
                        },
                        wgpu::VertexAttribute {
                            offset: std::mem::size_of::<[f32; 2]>() as wgpu::BufferAddress,
                            shader_location: 1,
                            format: wgpu::VertexFormat::Uint32,
                        },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: "fs_main",
                targets: &[Some(wgpu::ColorTargetState {
                    format: self.config.format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::LineStrip,
                strip_index_format: None,
                front_face: wgpu::FrontFace::Ccw,
                cull_mode: Some(wgpu::Face::Back),
                polygon_mode: wgpu::PolygonMode::Fill,
                unclipped_depth: false,
                conservative: false,
            },
            depth_stencil: Some(wgpu::DepthStencilState {
                format: wgpu::TextureFormat::Depth32Float,
                depth_write_enabled: true,
                depth_compare: wgpu::CompareFunction::Less,
                stencil: wgpu::StencilState::default(),
                bias: wgpu::DepthBiasState::default(),
            }),
            multisample: wgpu::MultisampleState {
                count: 4,
                mask: !0,
                alpha_to_coverage_enabled: false,
            },
            multiview: None,
        })
    }

    fn create_point_pipeline(&self) -> wgpu::RenderPipeline {
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("point_shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/point.wgsl").into()),
        });

        self.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("point_pipeline"),
            layout: None,
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: "vs_main",
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<PointVertex>() as wgpu::BufferAddress,
                    step_mode: wgpu::VertexStepMode::Vertex,
                    attributes: &[
                        wgpu::VertexAttribute {
                            offset: 0,
                            shader_location: 0,
                            format: wgpu::VertexFormat::Float32x2,
                        },
                        wgpu::VertexAttribute {
                            offset: std::mem::size_of::<[f32; 2]>() as wgpu::BufferAddress,
                            shader_location: 1,
                            format: wgpu::VertexFormat::Float32x4,
                        },
                        wgpu::VertexAttribute {
                            offset: (std::mem::size_of::<[f32; 2]>() + std::mem::size_of::<[f32; 4]>()) as wgpu::BufferAddress,
                            shader_location: 2,
                            format: wgpu::VertexFormat::Float32,
                        },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: "fs_main",
                targets: &[Some(wgpu::ColorTargetState {
                    format: self.config.format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                strip_index_format: None,
                front_face: wgpu::FrontFace::Ccw,
                cull_mode: None,
                polygon_mode: wgpu::PolygonMode::Fill,
                unclipped_depth: false,
                conservative: false,
            },
            depth_stencil: Some(wgpu::DepthStencilState {
                format: wgpu::TextureFormat::Depth32Float,
                depth_write_enabled: true,
                depth_compare: wgpu::CompareFunction::Less,
                stencil: wgpu::StencilState::default(),
                bias: wgpu::DepthBiasState::default(),
            }),
            multisample: wgpu::MultisampleState {
                count: 4,
                mask: !0,
                alpha_to_coverage_enabled: false,
            },
            multiview: None,
        })
    }

    fn create_shape_pipeline(&self) -> wgpu::RenderPipeline {
        let shader = self.device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("shape_shader"),
            source: wgpu::ShaderSource::Wgsl(include_str!("shaders/shape.wgsl").into()),
        });

        self.device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
            label: Some("shape_pipeline"),
            layout: None,
            vertex: wgpu::VertexState {
                module: &shader,
                entry_point: "vs_main",
                buffers: &[wgpu::VertexBufferLayout {
                    array_stride: std::mem::size_of::<ShapeVertex>() as wgpu::BufferAddress,
                    step_mode: wgpu::VertexStepMode::Vertex,
                    attributes: &[
                        wgpu::VertexAttribute {
                            offset: 0,
                            shader_location: 0,
                            format: wgpu::VertexFormat::Float32x2,
                        },
                        wgpu::VertexAttribute {
                            offset: std::mem::size_of::<[f32; 2]>() as wgpu::BufferAddress,
                            shader_location: 1,
                            format: wgpu::VertexFormat::Float32x4,
                        },
                    ],
                }],
                compilation_options: Default::default(),
            },
            fragment: Some(wgpu::FragmentState {
                module: &shader,
                entry_point: "fs_main",
                targets: &[Some(wgpu::ColorTargetState {
                    format: self.config.format,
                    blend: Some(wgpu::BlendState::ALPHA_BLENDING),
                    write_mask: wgpu::ColorWrites::ALL,
                })],
                compilation_options: Default::default(),
            }),
            primitive: wgpu::PrimitiveState {
                topology: wgpu::PrimitiveTopology::TriangleList,
                strip_index_format: None,
                front_face: wgpu::FrontFace::Ccw,
                cull_mode: Some(wgpu::Face::Back),
                polygon_mode: wgpu::PolygonMode::Fill,
                unclipped_depth: false,
                conservative: false,
            },
            depth_stencil: Some(wgpu::DepthStencilState {
                format: wgpu::TextureFormat::Depth32Float,
                depth_write_enabled: true,
                depth_compare: wgpu::CompareFunction::Less,
                stencil: wgpu::StencilState::default(),
                bias: wgpu::DepthBiasState::default(),
            }),
            multisample: wgpu::MultisampleState {
                count: 4,
                mask: !0,
                alpha_to_coverage_enabled: false,
            },
            multiview: None,
        })
    }
}

// GPU plot renderer wrapper
pub struct GpuPlotRenderer<'a> {
    gpu_renderer: Option<GpuRenderer<'a>>,
    fallback_available: bool,
    current_mode: RenderMode,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RenderMode {
    Gpu,
    Cpu,
    Auto,
}

impl<'a> GpuPlotRenderer<'a> {
    pub async fn new() -> Result<Self> {
        // Try to initialize GPU renderer
        let gpu_renderer = match pollster::block_on(async {
            // This would need a window reference, so we'll create a dummy one for now
            // In practice, this would be integrated with eframe's window
            None
        }) {
            Some(renderer) => Some(renderer),
            None => None,
        };

        Ok(Self {
            gpu_renderer,
            fallback_available: true,
            current_mode: RenderMode::Auto,
        })
    }

    pub fn render_line_chart(&mut self, points: &[Pos2], color: Color32, width: f32) -> Result<()> {
        match self.current_mode {
            RenderMode::Gpu => self.render_line_chart_gpu(points, color, width),
            RenderMode::Cpu => self.render_line_chart_cpu(points, color, width),
            RenderMode::Auto => {
                if self.gpu_renderer.is_some() {
                    self.render_line_chart_gpu(points, color, width)
                } else {
                    self.render_line_chart_cpu(points, color, width)
                }
            }
        }
    }

    fn render_line_chart_gpu(&mut self, points: &[Pos2], color: Color32, width: f32) -> Result<()> {
        if let Some(renderer) = &mut self.gpu_renderer {
            // Convert points to GPU vertices
            let vertices: Vec<LineVertex> = points
                .iter()
                .enumerate()
                .map(|(i, pos)| LineVertex {
                    position: [pos.x, pos.y],
                    strip_index: i as u32,
                })
                .collect();

            // Create GPU batch
            let buffer_id = renderer.create_line_batch(&vertices);

            // Create render command
            let commands = vec![RenderCommand::DrawLines {
                buffer_id,
                count: vertices.len() as u32,
                color: [color.r() as f32 / 255.0, color.g() as f32 / 255.0, color.b() as f32 / 255.0, color.a() as f32 / 255.0],
                width,
                style: LineStyle::Solid,
            }];

            // Render
            renderer.render(&commands)
        } else {
            Err(anyhow!("GPU renderer not available"))
        }
    }

    fn render_line_chart_cpu(&mut self, _points: &[Pos2], _color: Color32, _width: f32) -> Result<()> {
        // Fallback to CPU rendering using egui_plot
        // This would integrate with the existing plot system
        Ok(())
    }

    pub fn switch_to_gpu(&mut self) -> Result<()> {
        if self.gpu_renderer.is_some() {
            self.current_mode = RenderMode::Gpu;
            Ok(())
        } else {
            Err(anyhow!("GPU renderer not available"))
        }
    }

    pub fn switch_to_cpu(&mut self) {
        self.current_mode = RenderMode::Cpu;
    }

    pub fn auto_select_mode(&mut self, data_size: usize) {
        // Auto-select based on data size and GPU capabilities
        if let Some(renderer) = &self.gpu_renderer {
            let capabilities = renderer.capabilities();
            if data_size > 10000 && capabilities.max_vertices > data_size {
                self.current_mode = RenderMode::Gpu;
            } else {
                self.current_mode = RenderMode::Cpu;
            }
        } else {
            self.current_mode = RenderMode::Cpu;
        }
    }

    pub fn is_gpu_available(&self) -> bool {
        self.gpu_renderer.is_some()
    }

    pub fn get_capabilities(&self) -> Option<&GpuCapabilities> {
        self.gpu_renderer.as_ref().map(|r| r.capabilities())
    }
} 