struct VertexInput {
    @location(0) position: vec2<f32>,
    @location(1) color: vec4<f32>,
    @location(2) size: f32,
};

struct VertexOutput {
    @builtin(position) position: vec4<f32>,
    @location(0) color: vec4<f32>,
    @location(1) uv: vec2<f32>,
    @location(2) point_size: f32,
};

struct Uniforms {
    view_projection: mat4x4<f32>,
    point_size: f32,
};

@group(0) @binding(0) var<uniform> uniforms: Uniforms;

@vertex
fn vs_main(input: VertexInput) -> VertexOutput {
    let world_pos = vec4<f32>(input.position, 0.0, 1.0);
    let clip_pos = uniforms.view_projection * world_pos;
    
    // Generate UV coordinates for point shape
    let uv = vec2<f32>(0.5, 0.5); // Center of point
    
    return VertexOutput(
        clip_pos,
        input.color,
        uv,
        input.size * uniforms.point_size,
    );
}

@fragment
fn fs_main(input: VertexOutput) -> @location(0) vec4<f32> {
    // Circular point with anti-aliasing
    let distance = length(input.uv - vec2<f32>(0.5, 0.5));
    let radius = 0.5;
    
    if (distance > radius) {
        discard;
    }
    
    // Anti-aliased edge
    let alpha = 1.0 - smoothstep(radius - 0.1, radius, distance);
    
    return vec4<f32>(input.color.rgb, input.color.a * alpha);
} 